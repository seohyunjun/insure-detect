const fs = require('fs').promises;
const path = require('path');
const axios = require('axios');
const parquet = require('parquetjs');
const DuckDBQueryService = require('./duckdbQueryService');
require('dotenv').config();

// 컬럼명 정리 및 타입 정보 분리
function cleanColumnName(dirtyColumnName) {
    // 한글 컬럼명만 추출 (첫 번째 공백 전까지)
    const cleanName = dirtyColumnName.split(' ')[0];
    return cleanName;
}

function extractColumnType(dirtyColumnName) {
    // 타입 정보 추출 (VARCHAR, INTEGER 등)
    const typeMatch = dirtyColumnName.match(/(VARCHAR\(\d+\)|INTEGER|VARCHAR)/);
    if (typeMatch) {
        return typeMatch[1];
    }

    // 탭으로 구분된 형식도 확인
    const tabSeparated = dirtyColumnName.split('\t');
    if (tabSeparated.length > 1) {
        const lastPart = tabSeparated[tabSeparated.length - 1];
        if (lastPart.match(/(VARCHAR\(\d+\)|INTEGER|VARCHAR)/)) {
            return lastPart;
        }
    }

    return 'UNKNOWN';
}

function cleanDataArray(rawDataArray) {
    if (!rawDataArray || rawDataArray.length === 0) return { data: [], schema: {}, types: {} };

    const cleanedData = [];
    const columnTypes = {};
    const parquetSchema = {};

    // 첫 번째 데이터 항목에서 스키마 정보 추출
    const firstItem = rawDataArray[0];
    for (const [key] of Object.entries(firstItem)) {
        const cleanKey = cleanColumnName(key);
        const columnType = extractColumnType(key);

        columnTypes[cleanKey] = columnType;

        // Parquet 스키마 설정 (모든 필드를 UTF8로 저장하여 호환성 확보)
        parquetSchema[cleanKey] = { type: 'UTF8' };
    }

    // 모든 데이터 항목 정리
    for (const item of rawDataArray) {
        const cleanedItem = {};
        for (const [key, value] of Object.entries(item)) {
            const cleanKey = cleanColumnName(key);

            // 모든 값을 문자열로 변환하여 Parquet 호환성 확보
            if (value === null || value === undefined) {
                cleanedItem[cleanKey] = '';
            } else {
                cleanedItem[cleanKey] = String(value);
            }
        }
        cleanedData.push(cleanedItem);
    }

    return { data: cleanedData, schema: parquetSchema, types: columnTypes };
}

class DataCollector {
    constructor() {
        this.apiKey = process.env.API_KEY;
        this.baseUrl = process.env.API_BASE_URL || 'https://api.odcloud.kr/api';
        this.sourceDir = path.join(__dirname, '../../source/data');
        this.logsDir = path.join(__dirname, '../../source/logs');

        // 성능 최적화 설정
        this.optimizedReading = true;

        // DuckDB 질의 서비스
        this.duckDBService = new DuckDBQueryService();

        // 기본 UDDI (호환성 유지)
        this.uddis = {
            'pension_workplace': 'uddi:20ddf65d-51d8-421f-8ee5-b64f05554151'
        };

        // 동적으로 로드된 엔드포인트들
        this.dynamicUddis = {};
        this.uddisLoaded = false;

        // Retry 설정
        this.maxRetries = 3;
        this.retryDelay = 1000; // 1초 시작
        this.timeoutMs = 60000; // 60초로 증가

        if (!this.apiKey) {
            throw new Error('API_KEY가 설정되지 않았습니다. .env 파일을 확인해주세요.');
        }
    }

    async retryApiCall(apiCall, description, maxRetries = this.maxRetries) {
        let lastError;

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return await apiCall();
            } catch (error) {
                lastError = error;

                const isRetryableError =
                    error.code === 'ECONNABORTED' || // timeout
                    error.code === 'ENOTFOUND' ||   // DNS error
                    error.code === 'ECONNRESET' ||  // connection reset
                    (error.response && error.response.status >= 500) || // server errors
                    (error.response && error.response.status === 429);  // rate limit

                if (attempt === maxRetries || !isRetryableError) {
                    console.warn(`  ❌ ${description} 최종 실패 (시도 ${attempt}/${maxRetries}): ${error.message}`);
                    throw error;
                }

                const delay = this.retryDelay * Math.pow(2, attempt - 1); // exponential backoff
                console.warn(`  ⚠️ ${description} 실패 (시도 ${attempt}/${maxRetries}): ${error.message}`);
                console.log(`  ⏳ ${delay}ms 후 재시도...`);

                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }

        throw lastError;
    }

    async collectAllData(uddiName = 'pension_workplace', forceUpdate = false) {
        console.log(`🚀 ${uddiName} 데이터 수집을 시작합니다...`);

        // 동적 UDDI 로딩
        const allUddis = await this.loadDynamicUddis();
        const uddi = allUddis[uddiName];

        if (!uddi) {
            console.log(`❌ 지원하지 않는 UDDI: ${uddiName}`);
            console.log(`📋 사용 가능한 UDDI 목록:`);
            Object.keys(allUddis).forEach(key => {
                console.log(`  - ${key}`);
            });
            throw new Error(`지원하지 않는 UDDI: ${uddiName}`);
        }

        // 기존 데이터 파일 확인 (YYYY-MM 패턴으로 검색)
        if (!forceUpdate) {
            try {
                const files = await fs.readdir(this.sourceDir);
                const matchingFiles = files.filter(file =>
                    file.startsWith(`${uddiName}_`) &&
                    file.endsWith('.json') &&
                    file.match(/\d{4}-\d{2}\.json$/)
                );

                if (matchingFiles.length > 0) {
                    // 가장 최근 파일 찾기
                    const latestFile = matchingFiles
                        .map(file => ({
                            name: file,
                            path: path.join(this.sourceDir, file),
                            monthYear: file.match(/(\d{4}-\d{2})\.json$/)[1]
                        }))
                        .sort((a, b) => b.monthYear.localeCompare(a.monthYear))[0];

                    const stats = await fs.stat(latestFile.path);
                    const fileAge = new Date() - stats.mtime;
                    const ageInHours = fileAge / (1000 * 60 * 60);

                    if (ageInHours < 24) { // 24시간 이내의 데이터
                        console.log(`📄 기존 데이터 파일 발견: ${latestFile.name} (${Math.round(ageInHours)}시간 전)`);

                        // 기존 데이터 로드해서 확인
                        const existingData = JSON.parse(await fs.readFile(latestFile.path, 'utf8'));

                        if (existingData.data && existingData.data.length > 0) {
                            console.log(`✅ 기존 데이터 사용: ${existingData.data.length.toLocaleString()}개 레코드`);
                            console.log(`📅 수집 시간: ${existingData.metadata.collectedAt}`);
                            console.log(`💡 강제 업데이트를 원하면 forceUpdate=true 옵션을 사용하세요.`);

                            return {
                                success: true,
                                metadata: existingData.metadata,
                                recordCount: existingData.data.length,
                                dataFile: latestFile.path,
                                fromCache: true
                            };
                        } else {
                            console.log(`⚠️ 기존 파일이 비어있어 새로 수집합니다.`);
                        }
                    } else {
                        console.log(`⏰ 기존 데이터가 ${Math.round(ageInHours)}시간 전 것이므로 새로 수집합니다.`);
                    }
                } else {
                    console.log(`📥 기존 데이터 파일이 없으므로 새로 수집합니다.`);
                }
            } catch (error) {
                console.log(`⚠️ 기존 파일 확인 실패: ${error.message}`);
            }
        } else {
            console.log(`🔄 강제 업데이트 모드로 새로 수집합니다.`);
        }

        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const logFile = path.join(this.logsDir, `collect_${uddiName}_${timestamp}.log`);

        let allData = [];
        let page = 1;
        let totalPages = 1;
        let collectedCount = 0;

        try {
            // 첫 번째 페이지를 먼저 가져와서 총 페이지 수 확인
            console.log(`📡 첫 번째 페이지로 총 페이지 수 확인 중...`);

            // URL 구성 시 중복 방지
            let baseUrl;
            if (uddi.startsWith('15083277/v1/')) {
                baseUrl = `https://api.odcloud.kr/api/${uddi}`;
            } else {
                baseUrl = `${this.baseUrl}/${uddi}`;
            }
            console.log(`🔗 기본 URL: ${baseUrl}`);

            const firstResponse = await this.retryApiCall(
                () => axios.get(baseUrl, {
                    params: {
                        serviceKey: this.apiKey,
                        page: 1,
                        perPage: 1000
                    },
                    timeout: this.timeoutMs,
                    headers: {
                        'Accept': 'application/json',
                        'User-Agent': 'DataCollector/1.0'
                    }
                }),
                `첫 번째 페이지 조회`
            );

            // 첫 번째 응답으로 총 페이지 수 계산
            let totalCount = 0;
            if (firstResponse.data && firstResponse.data.data && Array.isArray(firstResponse.data.data)) {
                totalCount = firstResponse.data.totalCount || firstResponse.data.matchCount || 0;
                totalPages = Math.ceil(totalCount / 1000);
                allData.push(...firstResponse.data.data);
                collectedCount += firstResponse.data.data.length;
            } else if (firstResponse.data && Array.isArray(firstResponse.data)) {
                totalCount = firstResponse.data.length;
                totalPages = 1;
                allData.push(...firstResponse.data);
                collectedCount += firstResponse.data.length;
            } else {
                console.log(`⚠️ 예상치 못한 응답 형식`);
                totalPages = 1;
            }

            console.log(`📊 총 ${totalCount.toLocaleString()}개 레코드, ${totalPages}페이지 발견`);

            // 로그 기록
            const responseLogEntry = {
                timestamp: new Date().toISOString(),
                type: 'api_response',
                requestUrl: baseUrl,
                page: 1,
                responseStatus: firstResponse.status,
                responseStatusText: firstResponse.statusText,
                totalCount: totalCount,
                totalPages: totalPages
            };
            await this.appendLog(logFile, responseLogEntry);

            // 나머지 페이지들을 병렬로 처리 (배치 단위로)
            if (totalPages > 1) {
                const batchSize = 5; // 동시에 처리할 페이지 수
                const maxPages = Math.min(totalPages, 1000); // 최대 1000페이지까지

                for (let startPage = 2; startPage <= maxPages; startPage += batchSize) {
                    const endPage = Math.min(startPage + batchSize - 1, maxPages);
                    const pageRange = Array.from({length: endPage - startPage + 1}, (_, i) => startPage + i);

                    console.log(`🚀 페이지 ${startPage}-${endPage} 병렬 처리 중... (${pageRange.length}개 페이지)`);

                    // 페이지별 요청을 병렬로 실행
                    const pagePromises = pageRange.map(async (pageNum) => {
                        try {
                            const response = await this.retryApiCall(
                                () => axios.get(baseUrl, {
                                    params: {
                                        serviceKey: this.apiKey,
                                        page: pageNum,
                                        perPage: 1000
                                    },
                                    timeout: this.timeoutMs,
                                    headers: {
                                        'Accept': 'application/json',
                                        'User-Agent': 'DataCollector/1.0'
                                    }
                                }),
                                `페이지 ${pageNum} 조회`
                            );

                            let pageData = [];
                            if (response.data && response.data.data && Array.isArray(response.data.data)) {
                                pageData = response.data.data;
                            } else if (response.data && Array.isArray(response.data)) {
                                pageData = response.data;
                            }

                            console.log(`  ✅ 페이지 ${pageNum}: ${pageData.length}개 수집`);

                            return {
                                page: pageNum,
                                data: pageData,
                                success: true
                            };
                        } catch (error) {
                            console.warn(`  ❌ 페이지 ${pageNum} 최종 실패: ${error.message}`);
                            return {
                                page: pageNum,
                                data: [],
                                success: false,
                                error: error.message
                            };
                        }
                    });

                    // 배치 내 모든 페이지 요청 완료 대기
                    const batchResults = await Promise.all(pagePromises);

                    // 결과 처리
                    let batchSuccessCount = 0;
                    let batchFailedCount = 0;
                    for (const result of batchResults) {
                        if (result.success && result.data.length > 0) {
                            allData.push(...result.data);
                            collectedCount += result.data.length;
                            batchSuccessCount++;
                        } else if (!result.success) {
                            batchFailedCount++;
                            console.warn(`    ❌ 페이지 ${result.page} 영구 실패: ${result.error}`);
                        }
                    }

                    if (batchFailedCount > 0) {
                        console.log(`  📊 배치 결과: 성공 ${batchSuccessCount}개, 실패 ${batchFailedCount}개`);
                    }

                    // 메모리 사용량 확인 및 정리
                    const memoryUsage = this.getMemoryUsage();
                    console.log(`  📊 배치 완료: ${collectedCount.toLocaleString()}/${totalCount.toLocaleString()}개 수집 - 메모리: ${memoryUsage.usedMB}MB`);

                    if (memoryUsage.usedMB > 1000 && global.gc) {
                        global.gc();
                        const afterGC = this.getMemoryUsage();
                        console.log(`  🧹 가비지 컬렉션 후: ${afterGC.usedMB}MB`);
                    }

                    // 배치 간 딜레이 (API 제한 고려) - 더 긴 딜레이로 안정성 향상
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            }

            console.log(`🎉 병렬 수집 완료: 총 ${collectedCount.toLocaleString()}개 레코드`)

            // 데이터 정리 및 스키마 추출
            const { data: cleanedData, schema: parquetSchema, types: columnTypes } = cleanDataArray(allData);

            // summary에서 YYYY-MM 추출
            const dataMonthYear = this.extractDataPeriod(cleanedData);

            // 수집된 데이터를 파일에 저장 (parquet 형식)
            const dataFile = path.join(this.sourceDir, `${uddiName}_${dataMonthYear}.parquet`);
            const metadataFile = path.join(this.sourceDir, `${uddiName}_${dataMonthYear}_metadata.json`);

            const metadata = {
                uddi: uddi,
                uddiName: uddiName,
                collectedAt: new Date().toISOString(),
                totalRecords: cleanedData.length,
                totalPages: page - 1,
                version: '1.0',
                columnTypes: columnTypes,
                schema: parquetSchema
            };

            // Parquet 파일로 저장
            await this.writeDataToParquet(dataFile, cleanedData, parquetSchema);

            // 메타데이터는 별도 JSON 파일로 저장
            await fs.writeFile(metadataFile, JSON.stringify(metadata, null, 2), 'utf8');

            console.log(`\n🎉 데이터 수집 완료!`);
            console.log(`📊 총 ${allData.length}개 레코드 수집`);
            console.log(`💾 저장 위치: ${dataFile}`);

            // 최종 로그 기록
            const finalUrl = uddi.startsWith('15083277/v1/') ?
                `https://api.odcloud.kr/api/${uddi}` :
                `${this.baseUrl}/${uddi}`;

            const finalLog = {
                timestamp: new Date().toISOString(),
                status: 'completed',
                requestUrl: finalUrl,
                uddi: uddi,
                uddiName: uddiName,
                totalRecords: allData.length,
                totalPages: page - 1,
                dataFile: dataFile
            };
            await this.appendLog(logFile, finalLog);

            return {
                success: true,
                metadata: metadata,
                recordCount: allData.length,
                dataFile: dataFile
            };

        } catch (error) {
            console.error(`❌ 데이터 수집 중 오류 발생:`, error.message);

            // 오류 로그 기록 (더 자세한 정보 포함)
            const errorUrl = uddi.startsWith('15083277/v1/') ?
                `https://api.odcloud.kr/api/${uddi}` :
                `${this.baseUrl}/${uddi}`;

            const errorLog = {
                timestamp: new Date().toISOString(),
                type: 'error',
                status: 'error',
                requestUrl: errorUrl,
                uddi: uddi,
                uddiName: uddiName,
                error: {
                    message: error.message,
                    stack: error.stack,
                    code: error.code,
                    status: error.response?.status,
                    statusText: error.response?.statusText,
                    responseData: error.response?.data,
                    responseHeaders: error.response?.headers
                },
                collectedCount: collectedCount,
                lastPage: page,
                params: {
                    serviceKey: this.apiKey ? '***설정됨***' : '미설정',
                    page: page,
                    perPage: 1000
                },
                axiosConfig: {
                    method: 'GET',
                    url: errorUrl,
                    timeout: 30000
                }
            };
            await this.appendLog(logFile, errorLog);

            throw error;
        }
    }

    async writeDataToParquet(filePath, data, schema) {
        const startTime = Date.now();
        console.log(`💾 Parquet 파일로 저장 시작: ${filePath} (${data.length.toLocaleString()}개 레코드)`);

        try {
            // Parquet 스키마 생성
            const schemaStartTime = Date.now();
            const parquetSchema = new parquet.ParquetSchema(schema);
            const schemaTime = ((Date.now() - schemaStartTime) / 1000).toFixed(2);
            console.log(`  📝 스키마 생성 완료 (${schemaTime}초)`);

            // Parquet writer 생성
            const writerStartTime = Date.now();
            const writer = await parquet.ParquetWriter.openFile(parquetSchema, filePath);
            const writerTime = ((Date.now() - writerStartTime) / 1000).toFixed(2);
            console.log(`  📝 Writer 생성 완료 (${writerTime}초)`);

            // 데이터를 청크 단위로 저장
            const chunkSize = 1000;
            const totalItems = data.length;
            const writeStartTime = Date.now();

            for (let i = 0; i < totalItems; i += chunkSize) {
                const chunk = data.slice(i, i + chunkSize);

                for (const item of chunk) {
                    await writer.appendRow(item);
                }

                const elapsed = ((Date.now() - writeStartTime) / 1000).toFixed(1);
                const progress = ((Math.min(i + chunkSize, totalItems) / totalItems) * 100).toFixed(1);
                console.log(`  📝 진행률: ${Math.min(i + chunkSize, totalItems)}/${totalItems} (${progress}%, ${elapsed}초 경과)`);

                // 메모리 정리
                if (global.gc) {
                    global.gc();
                }
            }

            const closeStartTime = Date.now();
            await writer.close();
            const closeTime = ((Date.now() - closeStartTime) / 1000).toFixed(2);

            const endTime = Date.now();
            const totalTime = ((endTime - startTime) / 1000).toFixed(2);
            const writeTime = ((Date.now() - writeStartTime) / 1000).toFixed(2);

            console.log(`  📝 Writer 닫기 완룼 (${closeTime}초)`);
            console.log(`✅ Parquet 파일 저장 완료: ${totalItems.toLocaleString()}개 레코드 (총 ${totalTime}초, 쓰기: ${writeTime}초)`);

        } catch (error) {
            const endTime = Date.now();
            const totalTime = ((endTime - startTime) / 1000).toFixed(2);
            console.error(`❌ Parquet 파일 저장 실패 (${totalTime}초):`, error.message);
            throw error;
        }
    }

    getMemoryUsage() {
        const usage = process.memoryUsage();
        return {
            rss: usage.rss,
            heapTotal: usage.heapTotal,
            heapUsed: usage.heapUsed,
            external: usage.external,
            usedMB: Math.round(usage.heapUsed / 1024 / 1024),
            totalMB: Math.round(usage.heapTotal / 1024 / 1024),
            rssMB: Math.round(usage.rss / 1024 / 1024),
            used: Math.round(usage.heapUsed / 1024 / 1024) + 'MB'
        };
    }

    async appendLog(logFile, entry) {
        try {
            const logLine = JSON.stringify(entry) + '\n';
            await fs.appendFile(logFile, logLine, 'utf8');
        } catch (error) {
            console.error('로그 기록 실패:', error.message);
        }
    }

    async getAvailableData() {
        try {
            const files = await fs.readdir(this.sourceDir);
            const dataFiles = files.filter(file =>
                file.endsWith('.json') &&
                file.match(/\d{4}-\d{2}\.json$/)
            );

            const availableData = [];
            for (const file of dataFiles) {
                try {
                    const filePath = path.join(this.sourceDir, file);
                    const content = await fs.readFile(filePath, 'utf8');
                    const data = JSON.parse(content);

                    const monthYear = file.match(/(\d{4}-\d{2})\.json$/)[1];
                    const uddiName = file.replace(`_${monthYear}.json`, '');

                    availableData.push({
                        uddiName: uddiName,
                        monthYear: monthYear,
                        collectedAt: data.metadata.collectedAt,
                        recordCount: data.metadata.totalRecords,
                        file: file
                    });
                } catch (parseError) {
                    console.warn(`파일 파싱 실패: ${file}`, parseError.message);
                }
            }

            // 날짜순으로 정렬 (최신순)
            return availableData.sort((a, b) => b.monthYear.localeCompare(a.monthYear));
        } catch (error) {
            console.error('사용 가능한 데이터 조회 실패:', error.message);
            return [];
        }
    }

    async loadData(uddiName = 'pension_workplace') {
        const startTime = Date.now();
        console.log(`⏱️ 데이터 로드 시작: ${uddiName}`);

        try {
            // YYYY-MM 패턴으로 가장 최근 파일 찾기 (parquet 파일 우선)
            const files = await fs.readdir(this.sourceDir);

            // Parquet 파일 검색
            const parquetFiles = files.filter(file =>
                file.startsWith(`${uddiName}_`) &&
                file.endsWith('.parquet') &&
                file.match(/\d{4}-\d{2}\.parquet$/)
            );

            // JSON 파일 검색 (호환성을 위해)
            const jsonFiles = files.filter(file =>
                file.startsWith(`${uddiName}_`) &&
                file.endsWith('.json') &&
                file.match(/\d{4}-\d{2}\.json$/)
            );

            const allFiles = [
                ...parquetFiles.map(f => ({ name: f, type: 'parquet' })),
                ...jsonFiles.map(f => ({ name: f, type: 'json' }))
            ];

            if (allFiles.length === 0) {
                return {
                    success: false,
                    error: `${uddiName} 데이터 파일을 찾을 수 없습니다. 먼저 데이터를 수집해주세요.`
                };
            }

            // 가장 최근 파일 선택 (parquet 우선)
            const latestFile = allFiles
                .map(file => ({
                    name: file.name,
                    type: file.type,
                    path: path.join(this.sourceDir, file.name),
                    monthYear: file.name.match(/(\d{4}-\d{2})\.(parquet|json)$/)[1]
                }))
                .sort((a, b) => {
                    // 같은 날짜면 parquet 우선
                    if (a.monthYear === b.monthYear) {
                        return a.type === 'parquet' ? -1 : 1;
                    }
                    return b.monthYear.localeCompare(a.monthYear);
                })[0];

            let data, metadata;

            if (latestFile.type === 'parquet') {
                // 최적화된 Parquet 파일 로드
                const fileStartTime = Date.now();
                const reader = await parquet.ParquetReader.openFile(latestFile.path);
                const cursor = reader.getCursor();
                const records = [];

                let record = null;
                let count = 0;
                const batchSize = 5000; // 배치 크기로 메모리 관리

                console.log(`📖 Parquet 파일 로드 중: ${latestFile.name}`);

                while (record = await cursor.next()) {
                    records.push(record);
                    count++;

                    // 주기적으로 진행상황 표시
                    if (count % batchSize === 0) {
                        const memUsage = this.getMemoryUsage();
                        const elapsed = ((Date.now() - fileStartTime) / 1000).toFixed(1);
                        console.log(`  📊 ${count.toLocaleString()}개 레코드 로드됨 (${elapsed}초 경과, 메모리: ${memUsage.usedMB}MB)`);

                        // 가비지 컬렉션 실행
                        if (global.gc) {
                            global.gc();
                        }
                    }
                }

                await reader.close();
                const fileEndTime = Date.now();
                const fileLoadTime = ((fileEndTime - fileStartTime) / 1000).toFixed(2);
                console.log(`✅ Parquet 로드 완료: ${count.toLocaleString()}개 레코드 (${fileLoadTime}초)`);

                // 메타데이터 파일 로드
                const metadataPath = latestFile.path.replace('.parquet', '_metadata.json');
                const metadataContent = await fs.readFile(metadataPath, 'utf8');
                metadata = JSON.parse(metadataContent);

                data = records;
            } else {
                // JSON 파일 로드 (호환성)
                const content = await fs.readFile(latestFile.path, 'utf8');
                const jsonData = JSON.parse(content);
                metadata = jsonData.metadata;
                data = jsonData.data;
            }

            const endTime = Date.now();
            const totalLoadTime = ((endTime - startTime) / 1000).toFixed(2);
            console.log(`🎉 데이터 로드 완료: ${data.length.toLocaleString()}개 레코드 (총 ${totalLoadTime}초)`);

            return {
                success: true,
                metadata: metadata,
                data: data,
                fileType: latestFile.type,
                loadTime: totalLoadTime
            };
        } catch (error) {
            const endTime = Date.now();
            const totalTime = ((endTime - startTime) / 1000).toFixed(2);
            console.error(`❌ 데이터 로드 실패 (${totalTime}초):`, error.message);
            return {
                success: false,
                error: '데이터 로드 중 오류가 발생했습니다.',
                loadTime: totalTime
            };
        }
    }

    // 🦆 DuckDB SQL 질의 기반 데이터 로드 (최고 성능, 권장)
    async queryDataByDateRange(startDate, endDate, uddiName = 'pension_workplace', workplaceNameFilter = null) {
        console.log(`🦆 DuckDB SQL 질의로 기간별 데이터 로드: ${startDate} ~ ${endDate}`);

        try {
            const result = await this.duckDBService.queryDataByDateRange(startDate, endDate, workplaceNameFilter, uddiName);
            return result;
        } catch (error) {
            console.error('❌ DuckDB 질의 실패, 최적화 방식으로 폴백:', error.message);
            // DuckDB 실패 시 기존 최적화 방식으로 폴백
            return this.loadDataByDateRangeFast(startDate, endDate, uddiName, workplaceNameFilter);
        }
    }

    // 📊 사업장별 통계 질의
    async getWorkplaceStatistics(startDate, endDate, workplaceNameFilter = null) {
        console.log(`📊 DuckDB로 사업장 통계 질의: ${startDate} ~ ${endDate}`);

        try {
            return await this.duckDBService.getWorkplaceStats(startDate, endDate, workplaceNameFilter);
        } catch (error) {
            console.error('❌ 사업장 통계 질의 실패:', error.message);
            return {
                success: false,
                error: error.message,
                data: []
            };
        }
    }

    // 🔧 커스텀 SQL 질의 (고급 사용자용)
    async executeCustomSQL(sql, startDate = null, endDate = null) {
        console.log(`🔧 커스텀 SQL 질의 실행`);

        try {
            return await this.duckDBService.executeCustomQuery(sql, startDate, endDate);
        } catch (error) {
            console.error('❌ 커스텀 SQL 질의 실패:', error.message);
            return {
                success: false,
                error: error.message,
                data: []
            };
        }
    }

    // 🚀 고성능 최적화된 기간별 데이터 로드 (폴백용)
    async loadDataByDateRangeFast(startDate, endDate, uddiName = 'pension_workplace', workplaceNameFilter = null) {
        const overallStartTime = Date.now();
        console.log(`🚀 고성능 최적화 로더로 기간별 데이터 로드 시작: ${startDate} ~ ${endDate}`);

        if (workplaceNameFilter) {
            console.log(`🔍 사업장명 필터: ${workplaceNameFilter}`);
        }

        try {
            const files = await fs.readdir(this.sourceDir);

            // 기간 내의 모든 파일 찾기 (호환 파일 우선)
            const moment = require('moment');
            const start = moment(startDate, 'YYYY-MM');
            const end = moment(endDate, 'YYYY-MM');

            // 모든 기간 내 파일 찾기 (parquet 호환성은 나중에 체크)
            const potentialFiles = [];
            const parquetFiles = files.filter(file => {
                if (!file.endsWith('.parquet')) return false;

                // pension_workplace_YYYY-MM.parquet 패턴
                if (file.startsWith(`${uddiName}_`)) {
                    const match = file.match(/(\d{4}-\d{2})\.parquet$/);
                    if (match) {
                        const fileDate = moment(match[1], 'YYYY-MM');
                        if (fileDate.isBetween(start, end, null, '[]')) {
                            potentialFiles.push(file);
                            return true;
                        }
                    }
                }

                // pension_YYYY-MM_YYYY-MM.parquet 패턴
                if (file.startsWith('pension_')) {
                    const match = file.match(/pension_(\d{4}-\d{2})_\d{4}-\d{2}\.parquet$/);
                    if (match) {
                        const fileDate = moment(match[1], 'YYYY-MM');
                        if (fileDate.isBetween(start, end, null, '[]')) {
                            potentialFiles.push(file);
                            return true;
                        }
                    }
                }

                return false;
            });

            if (potentialFiles.length === 0) {
                return {
                    success: false,
                    error: `기간 ${startDate} ~ ${endDate} 내의 데이터 파일을 찾을 수 없습니다.`
                };
            }

            console.log(`📁 발견된 파일: ${potentialFiles.length}개`);

            let allData = [];
            let combinedMetadata = null;
            let totalProcessedRecords = 0;
            let successfulFiles = 0;
            let skippedFiles = 0;
            const compatibleFiles = [];
            const incompatibleFiles = [];

            // 모든 파일을 순차적으로 처리하면서 호환성 체크 (메모리 효율성)
            console.log(`⚡ 파일들 순차 처리 시작 (${potentialFiles.length}개)...`);

            for (const fileName of potentialFiles) {
                    const fileStartTime = Date.now();
                    const filePath = path.join(this.sourceDir, fileName);

                    try {
                        console.log(`  📖 ${fileName} 처리 시작...`);

                        // 먼저 parquet 파일 호환성 체크
                        let reader;
                        let isParquetCompatible = true;

                        try {
                            reader = await parquet.ParquetReader.openFile(filePath);
                        } catch (openError) {
                            if (typeof openError === 'string' && openError.includes('invalid parquet version')) {
                                isParquetCompatible = false;
                                console.log(`    ⚠️ ${fileName}: Parquet 버전 비호환, 메타데이터 방식으로 처리`);
                            } else {
                                throw openError;
                            }
                        }

                        if (isParquetCompatible) {
                            // 호환 가능한 파일: 기존 방식으로 처리
                            compatibleFiles.push(fileName);

                            const cursor = reader.getCursor();
                            let record = null;
                            let filteredCount = 0;
                            let recordCount = 0;

                            // 스트리밍 방식으로 직접 allData에 추가 (메모리 효율성)
                            const batchSize = 5000;

                            while (record = await cursor.next()) {
                                recordCount++;

                                if (workplaceNameFilter) {
                                    const workplaceName = record['사업장명'];
                                    if (!workplaceName || !workplaceName.toLowerCase().includes(workplaceNameFilter.toLowerCase())) {
                                        continue;
                                    }
                                }

                                allData.push(record); // 직접 최종 배열에 추가
                                filteredCount++;

                                // 메모리 관리
                                if (recordCount % batchSize === 0) {
                                    if (global.gc) {
                                        global.gc();
                                    }

                                    // 진행상황 표시
                                    if (recordCount % 20000 === 0) {
                                        const elapsed = ((Date.now() - fileStartTime) / 1000).toFixed(1);
                                        const memUsage = this.getMemoryUsage();
                                        console.log(`    📊 ${fileName}: ${recordCount.toLocaleString()}개 처리, ${filteredCount.toLocaleString()}개 필터링 (${elapsed}초, 메모리: ${memUsage.usedMB}MB)`);
                                    }
                                }
                            }

                            await reader.close();

                            const loadTime = ((Date.now() - fileStartTime) / 1000).toFixed(2);
                            console.log(`  ✅ ${fileName}: ${filteredCount.toLocaleString()}개 레코드 (${loadTime}초)`);

                            successfulFiles++;

                        } else {
                            // 비호환 파일: 메타데이터만 읽고 가능한 정보 제공
                            incompatibleFiles.push(fileName);

                            const metadataPath = filePath.replace('.parquet', '_metadata.json');
                            try {
                                const metadataContent = await fs.readFile(metadataPath, 'utf8');
                                const metadata = JSON.parse(metadataContent);

                                const loadTime = ((Date.now() - fileStartTime) / 1000).toFixed(2);
                                console.log(`  📄 ${fileName}: 메타데이터만 로드 (총 ${metadata.totalRecords?.toLocaleString() || '알 수 없음'}개 레코드, ${loadTime}초)`);

                                // 메타데이터 정보를 combinedMetadata에 추가
                                if (!combinedMetadata && metadata) {
                                    combinedMetadata = { ...metadata };
                                }

                            } catch (metaError) {
                                const loadTime = ((Date.now() - fileStartTime) / 1000).toFixed(2);
                                console.warn(`  ⚠️ ${fileName}: 메타데이터도 읽기 실패 (${loadTime}초)`);
                            }
                        }

                    } catch (error) {
                        const loadTime = ((Date.now() - fileStartTime) / 1000).toFixed(2);
                        console.warn(`  ❌ ${fileName}: 처리 실패 (${loadTime}초) - ${error.message}`);
                    }

                    // 파일 간 메모리 정리
                    if (global.gc) {
                        global.gc();
                    }
                }

            // 파일 처리 결과 요약
            skippedFiles = incompatibleFiles.length;
            console.log(`📊 처리 결과: 호환 ${compatibleFiles.length}개, 비호환 ${incompatibleFiles.length}개`);

            if (incompatibleFiles.length > 0) {
                console.log(`📄 비호환 파일들 (메타데이터만): ${incompatibleFiles.slice(0, 3).join(', ')}${incompatibleFiles.length > 3 ? '...' : ''}`);

                // 비호환 파일들에 대한 알림 메시지
                if (allData.length === 0 && workplaceNameFilter) {
                    console.log(`💡 알림: '${workplaceNameFilter}' 데이터가 2020년 이후 파일에 있을 수 있습니다.`);
                    console.log(`   - 이 파일들은 현재 parquet 버전 호환성 문제로 읽을 수 없습니다.`);
                    console.log(`   - 사용 가능한 연도: 2015-2019년`);
                }
            }

            // 메타데이터 생성
            if (!combinedMetadata) {
                combinedMetadata = {
                    uddiName: uddiName
                };
            }

            combinedMetadata.totalRecords = allData.length;
            combinedMetadata.totalProcessedRecords = allData.length;
            combinedMetadata.dateRange = { startDate, endDate };
            combinedMetadata.filesCount = potentialFiles.length;
            combinedMetadata.compatibleFiles = compatibleFiles.length;
            combinedMetadata.incompatibleFiles = incompatibleFiles.length;
            combinedMetadata.successfulFiles = successfulFiles;
            combinedMetadata.skippedFiles = skippedFiles;
            combinedMetadata.loadedAt = new Date().toISOString();
            combinedMetadata.workplaceNameFilter = workplaceNameFilter;
            combinedMetadata.loadMethod = 'Optimized with Compatibility Check';
            combinedMetadata.availableYears = '2015-2019 (fully compatible), 2020+ (metadata only)';

            // 사용자에게 유용한 정보 추가
            if (allData.length === 0 && incompatibleFiles.length > 0) {
                combinedMetadata.note = '요청한 기간의 데이터는 parquet 버전 호환성 문제로 현재 읽을 수 없습니다. 2015-2019년 데이터를 사용해보세요.';
            }

            const overallEndTime = Date.now();
            const totalLoadTime = ((overallEndTime - overallStartTime) / 1000).toFixed(2);
            console.log(`🎉 고성능 데이터 로드 완료: ${allData.length.toLocaleString()}개 레코드 (총 ${totalLoadTime}초)`);

            return {
                success: true,
                metadata: combinedMetadata,
                data: allData,
                filesLoaded: potentialFiles.length,
                compatibleFiles: compatibleFiles.length,
                incompatibleFiles: incompatibleFiles.length,
                successfulFiles: successfulFiles,
                skippedFiles: skippedFiles,
                totalLoadTime: totalLoadTime,
                method: 'Optimized with Compatibility Check'
            };

        } catch (error) {
            const overallEndTime = Date.now();
            const totalTime = ((overallEndTime - overallStartTime) / 1000).toFixed(2);
            console.error(`❌ 고성능 데이터 로드 실패 (${totalTime}초):`, error.message);

            // 폴백: 기존 방식 사용
            console.log(`🔄 기존 방식으로 폴백...`);
            return this.loadDataByDateRange(startDate, endDate, uddiName, workplaceNameFilter);
        }
    }

    // 🚀 고성능 최적화된 최신 데이터 로드 (권장)
    async loadDataFast(uddiName = 'pension_workplace') {
        const startTime = Date.now();
        console.log(`🚀 고성능 최적화 로더로 최신 데이터 로드: ${uddiName}`);

        try {
            // 가장 최근 호환 파일 찾기
            const files = await fs.readdir(this.sourceDir);
            const parquetFiles = files.filter(file =>
                file.startsWith(`${uddiName}_`) &&
                file.endsWith('.parquet') &&
                file.match(/\d{4}-\d{2}\.parquet$/)
            );

            if (parquetFiles.length === 0) {
                return {
                    success: false,
                    error: `${uddiName} parquet 파일을 찾을 수 없습니다.`
                };
            }

            // 호환 가능한 파일들 우선 선택
            const compatibleFiles = parquetFiles.filter(file => {
                const match = file.match(/(\d{4}-\d{2})\.parquet$/);
                if (match) {
                    const year = parseInt(match[1].substring(0, 4));
                    return year >= 2016 && year <= 2019;
                }
                return false;
            });

            const targetFiles = compatibleFiles.length > 0 ? compatibleFiles : parquetFiles;

            // 가장 최근 파일 선택
            const latestFile = targetFiles
                .map(file => ({
                    name: file,
                    path: path.join(this.sourceDir, file),
                    monthYear: file.match(/(\d{4}-\d{2})\.parquet$/)[1]
                }))
                .sort((a, b) => b.monthYear.localeCompare(a.monthYear))[0];

            console.log(`📖 최신 파일 고속 로드: ${latestFile.name}`);

            const reader = await parquet.ParquetReader.openFile(latestFile.path);
            const cursor = reader.getCursor();
            const data = [];
            let record = null;

            // 고속 읽기
            while (record = await cursor.next()) {
                data.push(record);
            }

            await reader.close();

            const endTime = Date.now();
            const totalLoadTime = ((endTime - startTime) / 1000).toFixed(2);
            console.log(`🎉 고성능 데이터 로드 완료: ${data.length.toLocaleString()}개 레코드 (총 ${totalLoadTime}초)`);

            // 메타데이터 로드
            let metadata;
            try {
                const metadataPath = latestFile.path.replace('.parquet', '_metadata.json');
                const metadataContent = await fs.readFile(metadataPath, 'utf8');
                metadata = JSON.parse(metadataContent);
                metadata.loadMethod = 'Optimized';
            } catch (metaError) {
                metadata = {
                    uddiName: uddiName,
                    totalRecords: data.length,
                    loadMethod: 'Optimized'
                };
            }

            return {
                success: true,
                metadata: metadata,
                data: data,
                fileType: 'parquet',
                loadTime: totalLoadTime,
                method: 'Optimized'
            };

        } catch (error) {
            const endTime = Date.now();
            const totalTime = ((endTime - startTime) / 1000).toFixed(2);
            console.error(`❌ 고성능 데이터 로드 실패 (${totalTime}초):`, error.message);

            // 폴백: 기존 방식 사용
            console.log(`🔄 기존 방식으로 폴백...`);
            return this.loadData(uddiName);
        }
    }

    // 📊 기존 방식: 기간별로 모든 파일을 로드하는 메서드 (스트리밍 방식으로 메모리 최적화)
    async loadDataByDateRange(startDate, endDate, uddiName = 'pension_workplace', workplaceNameFilter = null) {
        const overallStartTime = Date.now();
        console.log(`⏱️ 기간별 데이터 로드 시작: ${startDate} ~ ${endDate}`);
        if (workplaceNameFilter) {
            console.log(`🔍 사업장명 필터: ${workplaceNameFilter}`);
        }

        try {

            const files = await fs.readdir(this.sourceDir);

            // 기간 내의 모든 파일 찾기
            const moment = require('moment');
            const start = moment(startDate, 'YYYY-MM');
            const end = moment(endDate, 'YYYY-MM');

            // Parquet 파일 검색 (다양한 파일명 패턴 지원)
            const parquetFiles = files.filter(file => {
                if (!file.endsWith('.parquet')) return false;

                // pension_workplace_YYYY-MM.parquet 패턴
                if (file.startsWith(`${uddiName}_`)) {
                    const match = file.match(/(\d{4}-\d{2})\.parquet$/);
                    if (match) {
                        const fileDate = moment(match[1], 'YYYY-MM');
                        return fileDate.isBetween(start, end, null, '[]');
                    }
                }

                // pension_YYYY-MM_YYYY-MM.parquet 패턴 (동적 엔드포인트)
                if (file.startsWith('pension_')) {
                    const match = file.match(/pension_(\d{4}-\d{2})_\d{4}-\d{2}\.parquet$/);
                    if (match) {
                        const fileDate = moment(match[1], 'YYYY-MM');
                        return fileDate.isBetween(start, end, null, '[]');
                    }
                }

                return false;
            });

            // JSON 파일 검색 (호환성)
            const jsonFiles = files.filter(file => {
                if (!file.endsWith('.json') || file.includes('_metadata.json')) return false;

                // pension_workplace_YYYY-MM.json 패턴
                if (file.startsWith(`${uddiName}_`)) {
                    const match = file.match(/(\d{4}-\d{2})\.json$/);
                    if (match) {
                        const fileDate = moment(match[1], 'YYYY-MM');
                        return fileDate.isBetween(start, end, null, '[]');
                    }
                }

                // pension_YYYY-MM_YYYY-MM.json 패턴 (동적 엔드포인트)
                if (file.startsWith('pension_')) {
                    const match = file.match(/pension_(\d{4}-\d{2})_\d{4}-\d{2}\.json$/);
                    if (match) {
                        const fileDate = moment(match[1], 'YYYY-MM');
                        return fileDate.isBetween(start, end, null, '[]');
                    }
                }

                return false;
            });

            const allFiles = [
                ...parquetFiles.map(f => ({ name: f, type: 'parquet' })),
                ...jsonFiles.map(f => ({ name: f, type: 'json' }))
            ];

            if (allFiles.length === 0) {
                console.log(`⚠️ 기간 ${startDate} ~ ${endDate} 내의 데이터 파일을 찾을 수 없습니다.`);
                return {
                    success: false,
                    error: `기간 ${startDate} ~ ${endDate} 내의 데이터 파일을 찾을 수 없습니다.`
                };
            }

            console.log(`📁 발견된 파일: ${allFiles.length}개`);
            allFiles.forEach(file => console.log(`  - ${file.name}`));

            // 병렬로 파일들을 처리하여 성능 향상
            let allData = [];
            let combinedMetadata = null;
            let totalProcessedRecords = 0;

            // 파일을 병렬로 처리하기 위한 Promise 배열 생성
            const fileProcessingPromises = allFiles.map(async (fileInfo) => {
                const fileStartTime = Date.now();
                const filePath = path.join(this.sourceDir, fileInfo.name);

                try {
                    // 파일명에서 날짜 추출 (다양한 패턴 지원)
                    let monthYear;
                    if (fileInfo.name.startsWith('pension_workplace_')) {
                        monthYear = fileInfo.name.match(/(\d{4}-\d{2})\.(parquet|json)$/)?.[1];
                    } else if (fileInfo.name.startsWith('pension_')) {
                        monthYear = fileInfo.name.match(/pension_(\d{4}-\d{2})_\d{4}-\d{2}\.(parquet|json)$/)?.[1];
                    }

                    console.log(`📖 ${fileInfo.name} 로드 시작... (${monthYear})`);

                    let fileMetadata;
                    let fileData = [];
                    let filteredCount = 0;
                    let recordCount = 0;

                    if (fileInfo.type === 'parquet') {
                        // 최적화된 Parquet 파일 스트리밍 읽기
                        console.log(`    🔍 ${fileInfo.name}: Parquet 파일 열기 시도...`);

                        // Parquet 버전 호환성 확인을 위한 try-catch
                        let reader;
                        try {
                            reader = await parquet.ParquetReader.openFile(filePath);
                            console.log(`    ✅ ${fileInfo.name}: Parquet 파일 열기 성공`);
                        } catch (openError) {
                            // Parquet 버전 호환성 문제 처리
                            if (typeof openError === 'string' && openError.includes('invalid parquet version')) {
                                console.warn(`    ⚠️ ${fileInfo.name}: Parquet 버전 호환성 문제로 스킵 - ${openError}`);
                                // 메타데이터만 로드하고 빈 데이터로 처리
                                const metadataPath = filePath.replace('.parquet', '_metadata.json');
                                try {
                                    const metadataContent = await fs.readFile(metadataPath, 'utf8');
                                    fileMetadata = JSON.parse(metadataContent);
                                    console.log(`    📄 ${fileInfo.name}: 메타데이터만 로드하여 버전 호환성 문제 우회`);
                                } catch (metaError) {
                                    fileMetadata = { uddiName, monthYear, note: 'Parquet version incompatible' };
                                }

                                // 빈 데이터로 성공 처리 (스킵)
                                return {
                                    fileName: fileInfo.name,
                                    monthYear,
                                    data: [],
                                    metadata: fileMetadata,
                                    recordCount: 0,
                                    filteredCount: 0,
                                    loadTime: ((Date.now() - fileStartTime) / 1000).toFixed(2),
                                    success: true,
                                    error: null,
                                    skipped: true,
                                    skipReason: 'Parquet version incompatibility'
                                };
                            } else {
                                // 다른 종류의 에러는 그대로 throw
                                throw openError;
                            }
                        }

                        const cursor = reader.getCursor();
                        console.log(`    🔍 ${fileInfo.name}: 커서 생성 완료`);
                        const batchSize = 10000;

                        let record = null;
                        console.log(`    🔍 ${fileInfo.name}: 레코드 읽기 시작...`);

                        while (record = await cursor.next()) {
                            recordCount++;

                            // 사업장명 필터링 (제공된 경우에만)
                            if (workplaceNameFilter) {
                                const workplaceName = record['사업장명'];
                                if (!workplaceName || !workplaceName.toLowerCase().includes(workplaceNameFilter.toLowerCase())) {
                                    continue; // 조건에 맞지 않으면 스킵
                                }
                            }

                            fileData.push(record);
                            filteredCount++;

                            // 주기적으로 진행상황 표시 및 메모리 관리
                            if (recordCount % batchSize === 0) {
                                const memUsage = this.getMemoryUsage();
                                const elapsed = ((Date.now() - fileStartTime) / 1000).toFixed(1);
                                console.log(`    📊 ${fileInfo.name}: ${recordCount.toLocaleString()}개 처리, ${filteredCount.toLocaleString()}개 필터링 (${elapsed}초 경과, 메모리: ${memUsage.usedMB}MB)`);

                                // 메모리 사용량이 높으면 가비지 컬렉션 실행
                                if (memUsage.usedMB > 500 && global.gc) {
                                    global.gc();
                                }
                            }
                        }

                        console.log(`    🔍 ${fileInfo.name}: 레코드 읽기 완료, reader 닫기 시도...`);
                        await reader.close();
                        console.log(`    ✅ ${fileInfo.name}: reader 닫기 완료`);

                        // 메타데이터 파일 로드
                        const metadataPath = filePath.replace('.parquet', '_metadata.json');
                        console.log(`    🔍 ${fileInfo.name}: 메타데이터 파일 로드 시도: ${metadataPath}`);
                        try {
                            const metadataContent = await fs.readFile(metadataPath, 'utf8');
                            fileMetadata = JSON.parse(metadataContent);
                            console.log(`    ✅ ${fileInfo.name}: 메타데이터 로드 성공`);
                        } catch (metaError) {
                            console.warn(`⚠️ 메타데이터 파일 로드 실패: ${metadataPath}:`, metaError.message);
                            fileMetadata = { uddiName, monthYear };
                        }

                    } else {
                        // JSON 파일 처리 (호환성)
                        const content = await fs.readFile(filePath, 'utf8');
                        const jsonData = JSON.parse(content);
                        fileMetadata = jsonData.metadata;

                        // JSON 데이터도 스트리밍 방식으로 필터링
                        for (let i = 0; i < jsonData.data.length; i++) {
                            const record = jsonData.data[i];
                            recordCount++;

                            // 사업장명 필터링 (제공된 경우에만)
                            if (workplaceNameFilter) {
                                const workplaceName = record['사업장명'];
                                if (!workplaceName || !workplaceName.toLowerCase().includes(workplaceNameFilter.toLowerCase())) {
                                    continue; // 조건에 맞지 않으면 스킵
                                }
                            }

                            fileData.push(record);
                            filteredCount++;

                            // 주기적으로 진행상황 표시
                            if ((i + 1) % 10000 === 0) {
                                const elapsed = ((Date.now() - fileStartTime) / 1000).toFixed(1);
                                console.log(`    📊 ${fileInfo.name}: ${(i + 1).toLocaleString()}개 처리, ${filteredCount.toLocaleString()}개 필터링 (${elapsed}초 경과)`);
                            }
                        }
                    }

                    const fileEndTime = Date.now();
                    const fileLoadTime = ((fileEndTime - fileStartTime) / 1000).toFixed(2);
                    console.log(`  ✅ ${fileInfo.name}: ${filteredCount.toLocaleString()}개 레코드 수집 완료 (${fileLoadTime}초)`);

                    return {
                        fileName: fileInfo.name,
                        monthYear,
                        data: fileData,
                        metadata: fileMetadata,
                        recordCount,
                        filteredCount,
                        loadTime: fileLoadTime,
                        success: true,
                        error: null
                    };

                } catch (error) {
                    const fileEndTime = Date.now();
                    const fileLoadTime = ((fileEndTime - fileStartTime) / 1000).toFixed(2);

                    // 더 자세한 에러 정보 수집
                    let errorMessage = 'Unknown error';
                    let errorDetails = {};

                    if (error) {
                        // String 에러 처리 (parquetjs가 문자열 에러를 던지는 경우)
                        if (typeof error === 'string') {
                            errorMessage = error;
                            errorDetails = {
                                type: 'string',
                                original: error
                            };
                        } else {
                            // 일반적인 Error 객체 처리
                            errorMessage = error.message || error.toString() || 'Error object exists but no message';
                            errorDetails = {
                                name: error.name,
                                code: error.code,
                                stack: error.stack ? error.stack.split('\n')[0] : 'No stack trace',
                                type: typeof error,
                                constructor: error.constructor ? error.constructor.name : 'Unknown'
                            };
                        }
                    } else {
                        errorMessage = 'Error is null or undefined';
                    }

                    console.error(`❌ ${fileInfo.name} 처리 실패 (${fileLoadTime}초):`);
                    console.error(`   메시지: ${errorMessage}`);
                    console.error(`   상세정보:`, errorDetails);

                    return {
                        fileName: fileInfo.name,
                        monthYear: null,
                        data: [],
                        metadata: null,
                        recordCount: 0,
                        filteredCount: 0,
                        loadTime: fileLoadTime,
                        success: false,
                        error: errorMessage,
                        errorDetails: errorDetails
                    };
                }
            });

            // 모든 파일 처리를 병렬로 실행
            console.log(`🚀 ${allFiles.length}개 파일을 병렬로 처리 중...`);
            const fileResults = await Promise.all(fileProcessingPromises);

            // 결과를 합치기
            let successfulFiles = 0;
            let failedFiles = 0;
            let skippedFiles = 0;
            for (const result of fileResults) {
                if (result.success) {
                    if (result.skipped) {
                        console.log(`⏭️ ${result.fileName} 스킵됨: ${result.skipReason}`);
                        skippedFiles++;
                    } else {
                        allData.push(...result.data);
                        totalProcessedRecords += result.recordCount;
                        console.log(`🔗 ${result.fileName} 병합 완료: ${result.filteredCount.toLocaleString()}개 레코드`);
                    }

                    // 첫 번째 성공한 파일의 메타데이터를 기본으로 사용
                    if (!combinedMetadata && result.metadata) {
                        combinedMetadata = { ...result.metadata };
                    }

                    successfulFiles++;
                } else {
                    console.error(`❌ ${result.fileName} 처리 실패: ${result.error}`);
                    failedFiles++;
                }
            }

            if (failedFiles > 0 || skippedFiles > 0) {
                console.warn(`📊 처리 요약: 성공 ${successfulFiles}개, 실패 ${failedFiles}개, 스킵 ${skippedFiles}개`);
            }

            // 메모리 정리
            if (global.gc) {
                global.gc();
            }

            // 통합 메타데이터 생성
            if (!combinedMetadata) {
                combinedMetadata = {
                    uddiName: uddiName,
                    totalRecords: 0,
                    totalProcessedRecords: 0
                };
            }
            combinedMetadata.totalRecords = allData.length;
            combinedMetadata.totalProcessedRecords = totalProcessedRecords;
            combinedMetadata.dateRange = { startDate, endDate };
            combinedMetadata.filesCount = allFiles.length;
            combinedMetadata.successfulFiles = successfulFiles;
            combinedMetadata.failedFiles = failedFiles;
            combinedMetadata.skippedFiles = skippedFiles;
            combinedMetadata.loadedAt = new Date().toISOString();
            combinedMetadata.workplaceNameFilter = workplaceNameFilter;

            const overallEndTime = Date.now();
            const totalLoadTime = ((overallEndTime - overallStartTime) / 1000).toFixed(2);
            console.log(`🎉 기간별 데이터 로드 완료: ${allData.length.toLocaleString()}개 레코드 수집 (${totalProcessedRecords.toLocaleString()}개 중, ${allFiles.length}개 파일, 총 ${totalLoadTime}초)`);

            // 파일별 로드 시간 요약
            console.log(`📋 파일별 로드 시간 요약:`);
            fileResults.forEach(result => {
                if (result.success) {
                    console.log(`  ✅ ${result.fileName}: ${result.loadTime}초 (${result.filteredCount.toLocaleString()}개 레코드)`);
                } else {
                    console.log(`  ❌ ${result.fileName}: ${result.loadTime}초 (실패: ${result.error})`);
                }
            });

            return {
                success: true,
                metadata: combinedMetadata,
                data: allData,
                filesLoaded: allFiles.length,
                successfulFiles: successfulFiles,
                failedFiles: failedFiles,
                skippedFiles: skippedFiles,
                totalLoadTime: totalLoadTime,
                fileLoadTimes: fileResults.map(r => ({
                    fileName: r.fileName,
                    loadTime: r.loadTime,
                    success: r.success,
                    error: r.error,
                    skipped: r.skipped,
                    skipReason: r.skipReason
                }))
            };

        } catch (error) {
            const overallEndTime = Date.now();
            const totalTime = ((overallEndTime - overallStartTime) / 1000).toFixed(2);
            console.error(`❌ 기간별 데이터 로드 실패 (${totalTime}초):`, error.message);
            return {
                success: false,
                error: '기간별 데이터 로드 중 오류가 발생했습니다.',
                totalLoadTime: totalTime
            };
        }
    }

    extractDataPeriod(data) {
        if (!data || data.length === 0) {
            return new Date().toISOString().slice(0, 7); // YYYY-MM 형식
        }

        // 데이터에서 자료생성년월 필드를 찾아서 YYYY-MM 형식으로 변환
        const sampleItem = data[0];

        // 가능한 날짜 필드들 확인
        const dateFields = ['자료생성년월', 'stdrYm', 'baseYm', 'yearMonth'];

        for (const field of dateFields) {
            if (sampleItem[field]) {
                const dateValue = sampleItem[field].toString();

                // YYYYMM 형식인 경우
                if (dateValue.length === 6 && /^\d{6}$/.test(dateValue)) {
                    return `${dateValue.slice(0, 4)}-${dateValue.slice(4, 6)}`;
                }

                // YYYY-MM 형식인 경우
                if (dateValue.length === 7 && /^\d{4}-\d{2}$/.test(dateValue)) {
                    return dateValue;
                }
            }
        }

        // 모든 데이터를 확인해서 가장 최근 날짜 찾기
        const dates = data
            .map(item => {
                for (const field of dateFields) {
                    if (item[field]) {
                        const dateValue = item[field].toString();
                        if (dateValue.length === 6 && /^\d{6}$/.test(dateValue)) {
                            return `${dateValue.slice(0, 4)}-${dateValue.slice(4, 6)}`;
                        }
                        if (dateValue.length === 7 && /^\d{4}-\d{2}$/.test(dateValue)) {
                            return dateValue;
                        }
                    }
                }
                return null;
            })
            .filter(Boolean)
            .sort();

        return dates.length > 0 ? dates[dates.length - 1] : new Date().toISOString().slice(0, 7);
    }

    async loadDynamicUddis() {
        if (this.uddisLoaded) {
            return { ...this.uddis, ...this.dynamicUddis };
        }

        try {
            console.log('🔍 OpenAPI 문서에서 엔드포인트 정보를 로드합니다...');

            // OpenAPI 문서 가져오기
            const response = await this.retryApiCall(
                () => axios.get('https://infuser.odcloud.kr/oas/docs?namespace=15083277/v1', {
                    timeout: this.timeoutMs,
                    headers: {
                        'Accept': 'application/json',
                        'User-Agent': 'DataCollector/1.0'
                    }
                }),
                'OpenAPI 문서 조회'
            );

            if (response.data && response.data.paths) {
                const paths = response.data.paths;

                // /15083277/v1/uddi로 시작하는 path들만 필터링
                const validPaths = Object.keys(paths).filter(path =>
                    path.startsWith('/15083277/v1/uddi')
                );

                console.log(`📋 발견된 엔드포인트: ${validPaths.length}개`);

                let validEndpointCount = 0;
                let skippedEndpointCount = 0;

                // 각 path의 summary에서 YYYY-MM 추출
                for (const path of validPaths) {
                    const pathInfo = paths[path];

                    // GET 메서드의 summary 가져오기
                    const getSummary = pathInfo.get?.summary || pathInfo.get?.description || '';

                    if (getSummary) {
                        // LLM을 사용하여 summary에서 YYYY-MM 추출
                        const yearMonth = await this.extractYearMonthFromSummary(getSummary);

                        if (yearMonth) {
                            const pathWithoutSlash = path.substring(1); // 앞의 '/' 제거
                            const endpointKey = `pension_${yearMonth}`;
                            this.dynamicUddis[endpointKey] = pathWithoutSlash;

                            console.log(`✅ ${endpointKey}: ${getSummary.substring(0, 50)}...`);
                            validEndpointCount++;
                        } else {
                            console.log(`⚠️ ${path}: YYYY-MM 추출 실패 - ${getSummary.substring(0, 50)}...`);
                            skippedEndpointCount++;
                        }
                    }
                }

                this.uddisLoaded = true;
                console.log(`🎉 총 ${Object.keys(this.dynamicUddis).length}개 엔드포인트 로드 완료`);
                if (skippedEndpointCount > 0) {
                    console.log(`⏭️ ${skippedEndpointCount}개 엔드포인트 스킵됨 (무효한 데이터 형식)`);
                }
            }

        } catch (error) {
            console.warn('⚠️ 동적 엔드포인트 로드 실패:', error.message);
        }

        return { ...this.uddis, ...this.dynamicUddis };
    }

    async extractYearMonthFromSummary(summary) {
        try {
            // 모든 패턴을 파싱하도록 개선
            const allPatterns = [
                // 기본 년월 패턴
                /(\d{4})[년\-\/\.]\s*(\d{1,2})[월\-\/\.]?/g,
                /(\d{4})\s*년\s*(\d{1,2})\s*월/g,
                /(\d{4})\-(\d{2})/g,
                /(\d{4})\.(\d{1,2})/g,
                /(\d{4})\/(\d{1,2})/g,

                // YYYYMMDD 형식에서 YYYY-MM 추출
                /(\d{4})(\d{2})\d{2}$/,                       // 20210217 -> 2021-02

                // MM/DD/YYYY 형식
                /(\d{1,2})\/(\d{1,2})\/(\d{4})/,             // 09/24/2021 -> 2021-09

                // _MM/DD/YYYY 형식 (언더스코어로 시작)
                /_(\d{1,2})\/(\d{1,2})\/(\d{4})/,            // _09/24/2021 -> 2021-09

                // 특수 케이스: "2020년 5월_20200520" 형식
                /(\d{4})년\s*(\d{1,2})월.*_\d{8}/,           // 2020년 5월_20200520 -> 2020-05

                // 한글이 포함된 일반적인 패턴
                /(\d{4})년\s*(\d{1,2})월/,
                /(\d{4})\s+(\d{1,2})월/,

                // 엄격한 YYYYMM 패턴 (6자리)
                /(\d{4})(\d{2})(?![0-9])/                     // 202005 -> 2020-05 (뒤에 숫자가 오지 않는 경우)
            ];

            for (const regex of allPatterns) {
                const match = regex.exec(summary);
                if (match) {
                    let year, month;

                    // MM/DD/YYYY 또는 _MM/DD/YYYY 형식 처리
                    if (regex.source.includes('\\/.*\\/')) {
                        if (regex.source.startsWith('_')) {
                            // _MM/DD/YYYY 형식
                            month = match[1];
                            year = match[3];
                        } else {
                            // MM/DD/YYYY 형식
                            month = match[1];
                            year = match[3];
                        }
                    } else {
                        year = match[1];
                        month = match[2];
                    }

                    // 유효한 년도와 월인지 확인
                    const yearNum = parseInt(year);
                    const monthNum = parseInt(month);

                    if (yearNum >= 2015 && yearNum <= 2030 && monthNum >= 1 && monthNum <= 12) {
                        const formattedMonth = month.padStart(2, '0');
                        console.log(`📅 ${summary} -> ${year}-${formattedMonth}`);
                        return `${year}-${formattedMonth}`;
                    }
                }
            }

            // 특수 케이스 직접 처리
            const specialCases = {
                '국민연금공단_국민연금 가입 사업장 내역_09/24/2021': '2021-09',
                '국민연금공단_국민연금 가입 사업장 내역_10/22/2021': '2021-10'
            };

            if (specialCases[summary]) {
                const result = specialCases[summary];
                console.log(`📅 ${summary} -> ${result} (특수 케이스 매칭)`);
                return result;
            }

            // 8자리 날짜에서 YYYY-MM 추출 (마지막 시도)
            const dateMatch = summary.match(/(\d{8})/);
            if (dateMatch) {
                const dateStr = dateMatch[1];
                const year = dateStr.substring(0, 4);
                const month = dateStr.substring(4, 6);

                const yearNum = parseInt(year);
                const monthNum = parseInt(month);

                if (yearNum >= 2015 && yearNum <= 2030 && monthNum >= 1 && monthNum <= 12) {
                    console.log(`📅 ${summary} -> ${year}-${month} (8자리 날짜에서 추출)`);
                    return `${year}-${month}`;
                }
            }

            console.log(`⚠️ ${summary}: YYYY-MM 추출 실패`);
            return null;

        } catch (error) {
            console.warn('YYYY-MM 추출 중 오류:', error.message);
            return null;
        }
    }

    async askLLMForYearMonth(summary) {
        try {
            // WebFetch를 사용하여 Claude에게 질문
            const prompt = `다음 텍스트에서 년도와 월 정보를 찾아서 YYYY-MM 형식으로 추출해주세요.

텍스트: "${summary}"

응답 형식: YYYY-MM (예: 2024-03)
만약 날짜 정보를 찾을 수 없다면 "NOT_FOUND"라고 응답해주세요.`;

            // 실제로는 WebFetch나 다른 LLM API를 사용할 수 있지만
            // 여기서는 간단한 패턴 매칭으로 대체
            const patterns = [
                { regex: /2024.*?1월|2024.*?01|2024.*?January/i, result: '2024-01' },
                { regex: /2024.*?2월|2024.*?02|2024.*?February/i, result: '2024-02' },
                { regex: /2024.*?3월|2024.*?03|2024.*?March/i, result: '2024-03' },
                { regex: /2024.*?4월|2024.*?04|2024.*?April/i, result: '2024-04' },
                { regex: /2024.*?5월|2024.*?05|2024.*?May/i, result: '2024-05' },
                { regex: /2024.*?6월|2024.*?06|2024.*?June/i, result: '2024-06' },
                { regex: /2024.*?7월|2024.*?07|2024.*?July/i, result: '2024-07' },
                { regex: /2024.*?8월|2024.*?08|2024.*?August/i, result: '2024-08' },
                { regex: /2024.*?9월|2024.*?09|2024.*?September/i, result: '2024-09' },
                { regex: /2024.*?10월|2024.*?10|2024.*?October/i, result: '2024-10' },
                { regex: /2024.*?11월|2024.*?11|2024.*?November/i, result: '2024-11' },
                { regex: /2024.*?12월|2024.*?12|2024.*?December/i, result: '2024-12' },

                // 2023년도
                { regex: /2023.*?1월|2023.*?01|2023.*?January/i, result: '2023-01' },
                { regex: /2023.*?2월|2023.*?02|2023.*?February/i, result: '2023-02' },
                { regex: /2023.*?3월|2023.*?03|2023.*?March/i, result: '2023-03' },
                { regex: /2023.*?4월|2023.*?04|2023.*?April/i, result: '2023-04' },
                { regex: /2023.*?5월|2023.*?05|2023.*?May/i, result: '2023-05' },
                { regex: /2023.*?6월|2023.*?06|2023.*?June/i, result: '2023-06' },
                { regex: /2023.*?7월|2023.*?07|2023.*?July/i, result: '2023-07' },
                { regex: /2023.*?8월|2023.*?08|2023.*?August/i, result: '2023-08' },
                { regex: /2023.*?9월|2023.*?09|2023.*?September/i, result: '2023-09' },
                { regex: /2023.*?10월|2023.*?10|2023.*?October/i, result: '2023-10' },
                { regex: /2023.*?11월|2023.*?11|2023.*?November/i, result: '2023-11' },
                { regex: /2023.*?12월|2023.*?12|2023.*?December/i, result: '2023-12' }
            ];

            for (const pattern of patterns) {
                if (pattern.regex.test(summary)) {
                    return pattern.result;
                }
            }

            return null;
        } catch (error) {
            console.warn('LLM 호출 실패:', error.message);
            return null;
        }
    }

    async getAllAvailableEndpoints() {
        try {
            console.log('🔍 15083277 namespace의 사용 가능한 엔드포인트를 조회합니다...');

            // OpenAPI 문서에서 엔드포인트 정보 가져오기
            const response = await this.retryApiCall(
                () => axios.get('https://infuser.odcloud.kr/oas/docs?namespace=15083277/v1', {
                    timeout: this.timeoutMs,
                    headers: {
                        'Accept': 'application/json',
                        'User-Agent': 'DataCollector/1.0'
                    }
                }),
                'OpenAPI 문서 조회 (getAllAvailableEndpoints)'
            );

            // 응답에서 paths 정보 추출
            let endpoints = {};

            if (response.data && response.data.paths) {
                const paths = response.data.paths;

                Object.keys(paths).forEach(path => {
                    // path가 /15083277/v1/uddi:... 형식인지 확인
                    const match = path.match(/\/15083277\/v1\/(uddi:[a-f0-9-]+_\d+)/);
                    if (match) {
                        const uddiPart = match[1];
                        const endpointKey = `endpoint_${uddiPart.split('_')[1] || Date.now()}`;
                        endpoints[endpointKey] = `15083277/v1/${uddiPart}`;
                    }
                });
            }

            console.log(`✅ ${Object.keys(endpoints).length}개의 엔드포인트를 발견했습니다.`);
            return endpoints;

        } catch (error) {
            console.warn('⚠️ 엔드포인트 조회 실패, 기본 엔드포인트 사용:', error.message);
            return {};
        }
    }

    async getExistingParquetMonths() {
        try {
            const files = await fs.readdir(this.sourceDir);
            const monthsSet = new Set();

            // parquet 파일들에서 년월 추출
            files.forEach(file => {
                if (file.endsWith('.parquet')) {
                    // pension_YYYY-MM_YYYY-MM.parquet 패턴
                    const match = file.match(/pension_(\d{4}-\d{2})_\d{4}-\d{2}\.parquet$/);
                    if (match) {
                        monthsSet.add(match[1]);
                    }

                    // pension_workplace_YYYY-MM.parquet 패턴
                    const workplaceMatch = file.match(/pension_workplace_(\d{4}-\d{2})\.parquet$/);
                    if (workplaceMatch) {
                        monthsSet.add(workplaceMatch[1]);
                    }
                }
            });

            return Array.from(monthsSet).sort();
        } catch (error) {
            console.warn('⚠️ 기존 parquet 파일 조회 실패:', error.message);
            return [];
        }
    }

    async collectAllAvailableData() {
        console.log('🚀 모든 사용 가능한 엔드포인트에서 데이터 수집을 시작합니다...');

        // 동적으로 UDDI 로딩
        const allEndpoints = await this.loadDynamicUddis();

        // 기존 parquet 파일들에서 이미 수집된 년월 추출
        const existingMonths = await this.getExistingParquetMonths();
        console.log(`📄 기존 parquet 파일이 있는 달: ${existingMonths.join(', ')}`);

        const results = [];

        for (const [endpointName, endpointPath] of Object.entries(allEndpoints)) {
            if (endpointName === 'namespace_15083277') continue; // 베이스 패턴 스킵

            // endpointName에서 년월 추출 (pension_YYYY-MM 형식)
            const monthMatch = endpointName.match(/pension_(\d{4}-\d{2})/);
            if (monthMatch) {
                const endpointMonth = monthMatch[1];
                if (existingMonths.includes(endpointMonth)) {
                    console.log(`⏭️ ${endpointName} 스킵: parquet 파일이 이미 존재함 (${endpointMonth})`);
                    results.push({
                        endpoint: endpointName,
                        success: true,
                        recordCount: 0,
                        error: null,
                        skipped: true,
                        reason: 'parquet file exists'
                    });
                    continue;
                }
            }

            try {
                console.log(`\n📡 ${endpointName} 수집 중...`);
                const result = await this.collectAllData(endpointName);
                results.push({
                    endpoint: endpointName,
                    success: result.success,
                    recordCount: result.recordCount || 0,
                    error: result.error || null
                });
            } catch (error) {
                console.error(`❌ ${endpointName} 수집 실패:`, error.message);
                results.push({
                    endpoint: endpointName,
                    success: false,
                    recordCount: 0,
                    error: error.message
                });
            }
        }

        console.log('\n🎉 모든 엔드포인트 수집 완료!');
        console.log('='.repeat(50));
        results.forEach(result => {
            if (result.skipped) {
                console.log(`⏭️ ${result.endpoint}: 스킵됨 (${result.reason})`);
            } else {
                const status = result.success ? '✅' : '❌';
                console.log(`${status} ${result.endpoint}: ${result.recordCount.toLocaleString()}개 레코드`);
                if (result.error) {
                    console.log(`   오류: ${result.error}`);
                }
            }
        });

        return results;
    }

    async cleanupOldFiles(daysToKeep = 30) {
        try {
            const files = await fs.readdir(this.sourceDir);
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);

            let deletedCount = 0;

            for (const file of files) {
                // .json 파일만 처리
                if (!file.endsWith('.json')) continue;

                const filePath = path.join(this.sourceDir, file);
                const stats = await fs.stat(filePath);

                if (stats.mtime < cutoffDate) {
                    await fs.unlink(filePath);
                    deletedCount++;
                    console.log(`🗑️ 오래된 파일 삭제: ${file}`);
                }
            }

            console.log(`📁 정리 완료: ${deletedCount}개 파일 삭제`);
            return deletedCount;
        } catch (error) {
            console.error('파일 정리 실패:', error.message);
            return 0;
        }
    }

    async exportSummaryParquetMapping() {
        console.log('📊 OpenAPI summary와 parquet 파일 매핑을 CSV로 저장합니다...');

        try {
            // temp 디렉토리 확인 및 생성
            const tempDir = path.join(__dirname, '../../temp');
            try {
                await fs.access(tempDir);
            } catch {
                await fs.mkdir(tempDir, { recursive: true });
                console.log(`📁 temp 디렉토리 생성: ${tempDir}`);
            }

            // OpenAPI 문서에서 엔드포인트 정보 가져오기
            const response = await this.retryApiCall(
                () => axios.get('https://infuser.odcloud.kr/oas/docs?namespace=15083277/v1', {
                    timeout: this.timeoutMs,
                    headers: {
                        'Accept': 'application/json',
                        'User-Agent': 'DataCollector/1.0'
                    }
                }),
                'OpenAPI 문서 조회 (exportSummaryParquetMapping)'
            );

            if (!response.data || !response.data.paths) {
                throw new Error('OpenAPI 문서에서 paths 정보를 찾을 수 없습니다.');
            }

            // 기존 parquet 파일 목록 가져오기
            const sourceFiles = await fs.readdir(this.sourceDir);
            const parquetFiles = sourceFiles.filter(file => file.endsWith('.parquet'));

            console.log(`📋 발견된 parquet 파일: ${parquetFiles.length}개`);

            const paths = response.data.paths;
            const validPaths = Object.keys(paths).filter(path =>
                path.startsWith('/15083277/v1/uddi')
            );

            console.log(`📋 발견된 API 엔드포인트: ${validPaths.length}개`);

            const mappingData = [];

            // 각 path의 summary 분석
            for (const path of validPaths) {
                const pathInfo = paths[path];
                const getSummary = pathInfo.get?.summary || pathInfo.get?.description || '';

                if (getSummary) {
                    // YYYY-MM 추출 시도
                    const yearMonth = await this.extractYearMonthFromSummary(getSummary);

                    // 해당하는 parquet 파일 찾기
                    let matchingParquetFiles = [];
                    if (yearMonth) {
                        // pension_YYYY-MM_YYYY-MM.parquet 패턴으로 찾기
                        matchingParquetFiles = parquetFiles.filter(file =>
                            file.includes(`pension_${yearMonth}_`) ||
                            file.includes(`pension_workplace_${yearMonth}`)
                        );
                    }

                    mappingData.push({
                        endpoint_path: path,
                        summary: getSummary,
                        extracted_year_month: yearMonth || '',
                        matching_parquet_files: matchingParquetFiles.join('; '),
                        parquet_file_count: matchingParquetFiles.length,
                        status: yearMonth ? 'valid' : 'parse_failed'
                    });
                } else {
                    mappingData.push({
                        endpoint_path: path,
                        summary: '',
                        extracted_year_month: '',
                        matching_parquet_files: '',
                        parquet_file_count: 0,
                        status: 'no_summary'
                    });
                }
            }

            // CSV 헤더
            const csvHeaders = [
                'endpoint_path',
                'summary',
                'extracted_year_month',
                'matching_parquet_files',
                'parquet_file_count',
                'status'
            ];

            // CSV 데이터 생성
            const csvRows = [csvHeaders.join(',')];

            for (const item of mappingData) {
                const row = [
                    `"${item.endpoint_path}"`,
                    `"${item.summary.replace(/"/g, '""')}"`, // CSV escape
                    `"${item.extracted_year_month}"`,
                    `"${item.matching_parquet_files}"`,
                    item.parquet_file_count,
                    `"${item.status}"`
                ];
                csvRows.push(row.join(','));
            }

            // CSV 파일 저장
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const csvFilePath = path.join(tempDir, `summary_parquet_mapping_${timestamp}.csv`);
            const csvContent = csvRows.join('\n');

            await fs.writeFile(csvFilePath, csvContent, 'utf8');

            // 통계 출력
            const validCount = mappingData.filter(item => item.status === 'valid').length;
            const parseFailedCount = mappingData.filter(item => item.status === 'parse_failed').length;
            const noSummaryCount = mappingData.filter(item => item.status === 'no_summary').length;
            const withParquetCount = mappingData.filter(item => item.parquet_file_count > 0).length;

            console.log('\n📊 매핑 결과 통계:');
            console.log(`✅ 유효한 엔드포인트: ${validCount}개`);
            console.log(`❌ 파싱 실패: ${parseFailedCount}개`);
            console.log(`❓ summary 없음: ${noSummaryCount}개`);
            console.log(`📄 parquet 파일 매칭: ${withParquetCount}개`);
            console.log(`💾 CSV 파일 저장: ${csvFilePath}`);

            return {
                success: true,
                csvFilePath: csvFilePath,
                totalEndpoints: mappingData.length,
                validEndpoints: validCount,
                parseFailedCount: parseFailedCount,
                withParquetFiles: withParquetCount
            };

        } catch (error) {
            console.error('❌ CSV 매핑 파일 생성 실패:', error.message);
            return {
                success: false,
                error: error.message
            };
        }
    }
}

module.exports = DataCollector;