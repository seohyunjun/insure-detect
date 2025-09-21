const fs = require('fs').promises;
const path = require('path');
const axios = require('axios');
const parquet = require('parquetjs');
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

        // 기본 UDDI (호환성 유지)
        this.uddis = {
            'pension_workplace': 'uddi:20ddf65d-51d8-421f-8ee5-b64f05554151'
        };

        // 동적으로 로드된 엔드포인트들
        this.dynamicUddis = {};
        this.uddisLoaded = false;

        if (!this.apiKey) {
            throw new Error('API_KEY가 설정되지 않았습니다. .env 파일을 확인해주세요.');
        }
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
            do {
                console.log(`📡 페이지 ${page}/${totalPages} 수집 중...`);

                // URL 구성 시 중복 방지
                let url;
                if (uddi.startsWith('15083277/v1/')) {
                    // 15083277 namespace 엔드포인트인 경우
                    url = `https://api.odcloud.kr/api/${uddi}`;
                } else {
                    // 기존 uddi 형식인 경우
                    url = `${this.baseUrl}/${uddi}`;
                }
                console.log(`🔗 요청 URL: ${url}`);

                const response = await axios.get(url, {
                    params: {
                        serviceKey: this.apiKey,
                        page: page,
                        perPage: 1000
                    },
                    timeout: 30000,
                    headers: {
                        'Accept': 'application/json',
                        'User-Agent': 'DataCollector/1.0'
                    }
                });

                let pageData = [];
                let pageCount = 0;
                let totalCount = 0;

                // API 응답을 로그에 기록 (처음 몇 글자만)
                const responsePreview = JSON.stringify(response.data).substring(0, 500);
                // console.log(`📄 API 응답 미리보기: ${responsePreview}...`);

                // 응답을 로그 파일에 기록
                const responseLogEntry = {
                    timestamp: new Date().toISOString(),
                    type: 'api_response',
                    requestUrl: url,
                    page: page,
                    responseStatus: response.status,
                    responseStatusText: response.statusText,
                    responseHeaders: response.headers,
                    responsePreview: responsePreview,
                    fullResponseSize: JSON.stringify(response.data).length
                };
                await this.appendLog(logFile, responseLogEntry);

                // Open Data Cloud API 응답 형식 처리
                if (response.data && response.data.data && Array.isArray(response.data.data)) {
                    pageData = response.data.data;
                    pageCount = response.data.currentCount || pageData.length;
                    totalCount = response.data.totalCount || response.data.matchCount || 0;
                    totalPages = Math.ceil(totalCount / 1000);
                } else if (response.data && Array.isArray(response.data)) {
                    pageData = response.data;
                    pageCount = pageData.length;
                    totalCount = pageData.length;
                    totalPages = 1;
                } else {
                    console.log(`⚠️ 페이지 ${page}: 예상치 못한 응답 형식`);

                    // 예상치 못한 응답 형식을 로그에 기록
                    const unexpectedResponseLog = {
                        timestamp: new Date().toISOString(),
                        type: 'unexpected_response',
                        requestUrl: url,
                        page: page,
                        responseData: response.data,
                        responseType: typeof response.data,
                        responseKeys: response.data ? Object.keys(response.data) : null,
                        isArray: Array.isArray(response.data),
                        hasDataProperty: response.data && response.data.hasOwnProperty('data')
                    };
                    await this.appendLog(logFile, unexpectedResponseLog);
                    break;
                }

                if (pageData.length > 0) {
                    allData.push(...pageData);
                    collectedCount += pageData.length;

                    // 메모리 사용량 확인
                    const memoryUsage = this.getMemoryUsage();
                    console.log(`  ✅ ${pageData.length}개 데이터 수집 (누적: ${collectedCount}/${totalCount}) - 메모리: ${memoryUsage.usedMB}MB`);

                    // 메모리 사용량이 너무 높으면 경고
                    if (memoryUsage.usedMB > 1000) {
                        console.log(`  ⚠️ 높은 메모리 사용량 감지: ${memoryUsage.usedMB}MB`);
                        if (global.gc) {
                            global.gc();
                            const afterGC = this.getMemoryUsage();
                            console.log(`  🧹 가비지 컬렉션 후: ${afterGC.usedMB}MB`);
                        }
                    }

                    // 진행 상황을 로그 파일에 기록
                    const logEntry = {
                        timestamp: new Date().toISOString(),
                        requestUrl: url,
                        uddi: uddi,
                        page: page,
                        pageCount: pageData.length,
                        totalCollected: collectedCount,
                        totalCount: totalCount,
                        params: {
                            serviceKey: this.apiKey ? '***설정됨***' : '미설정',
                            page: page,
                            perPage: 1000
                        }
                    };
                    await this.appendLog(logFile, logEntry);
                } else {
                    console.log(`  ⚠️ 페이지 ${page}: 데이터 없음`);
                    break;
                }

                page++;

                // API 호출 제한을 고려한 딜레이
                await new Promise(resolve => setTimeout(resolve, 100));

            } while (page <= totalPages && page <= 1000); // 최대 1000페이지까지만

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
        try {
            console.log(`💾 Parquet 파일로 저장 중: ${filePath}`);

            // Parquet 스키마 생성
            const parquetSchema = new parquet.ParquetSchema(schema);

            // Parquet writer 생성
            const writer = await parquet.ParquetWriter.openFile(parquetSchema, filePath);

            // 데이터를 청크 단위로 저장
            const chunkSize = 1000;
            const totalItems = data.length;

            for (let i = 0; i < totalItems; i += chunkSize) {
                const chunk = data.slice(i, i + chunkSize);

                for (const item of chunk) {
                    await writer.appendRow(item);
                }

                console.log(`  📝 진행률: ${Math.min(i + chunkSize, totalItems)}/${totalItems}`);

                // 메모리 정리
                if (global.gc) {
                    global.gc();
                }
            }

            await writer.close();
            console.log(`✅ Parquet 파일 저장 완료: ${totalItems}개 레코드`);

        } catch (error) {
            console.error('Parquet 파일 저장 실패:', error.message);
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
                // Parquet 파일 로드
                const reader = await parquet.ParquetReader.openFile(latestFile.path);
                const cursor = reader.getCursor();
                const records = [];

                let record = null;
                while (record = await cursor.next()) {
                    records.push(record);
                }

                await reader.close();

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

            return {
                success: true,
                metadata: metadata,
                data: data,
                fileType: latestFile.type
            };
        } catch (error) {
            console.error('데이터 로드 실패:', error.message);
            return {
                success: false,
                error: '데이터 로드 중 오류가 발생했습니다.'
            };
        }
    }

    // 기간별로 모든 파일을 로드하는 새로운 메서드
    async loadDataByDateRange(startDate, endDate, uddiName = 'pension_workplace') {
        try {
            console.log(`📅 기간별 데이터 로드: ${startDate} ~ ${endDate}`);

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

            // 모든 파일의 데이터를 합치기
            let allData = [];
            let combinedMetadata = null;

            for (const fileInfo of allFiles) {
                const filePath = path.join(this.sourceDir, fileInfo.name);

                // 파일명에서 날짜 추출 (다양한 패턴 지원)
                let monthYear;
                if (fileInfo.name.startsWith('pension_workplace_')) {
                    monthYear = fileInfo.name.match(/(\d{4}-\d{2})\.(parquet|json)$/)?.[1];
                } else if (fileInfo.name.startsWith('pension_')) {
                    monthYear = fileInfo.name.match(/pension_(\d{4}-\d{2})_\d{4}-\d{2}\.(parquet|json)$/)?.[1];
                }

                console.log(`📖 ${fileInfo.name} 로드 중... (${monthYear})`);

                let fileData, fileMetadata;

                if (fileInfo.type === 'parquet') {
                    // Parquet 파일 로드
                    const reader = await parquet.ParquetReader.openFile(filePath);
                    const cursor = reader.getCursor();
                    const records = [];

                    let record = null;
                    while (record = await cursor.next()) {
                        records.push(record);
                    }

                    await reader.close();

                    // 메타데이터 파일 로드
                    const metadataPath = filePath.replace('.parquet', '_metadata.json');
                    try {
                        const metadataContent = await fs.readFile(metadataPath, 'utf8');
                        fileMetadata = JSON.parse(metadataContent);
                    } catch (metaError) {
                        console.warn(`⚠️ 메타데이터 파일 로드 실패: ${metadataPath}`);
                        fileMetadata = { uddiName, monthYear };
                    }

                    fileData = records;
                } else {
                    // JSON 파일 로드
                    const content = await fs.readFile(filePath, 'utf8');
                    const jsonData = JSON.parse(content);
                    fileMetadata = jsonData.metadata;
                    fileData = jsonData.data;
                }

                allData = allData.concat(fileData);

                // 첫 번째 파일의 메타데이터를 기본으로 사용
                if (!combinedMetadata) {
                    combinedMetadata = { ...fileMetadata };
                }

                console.log(`  ✅ ${fileData.length}개 레코드 로드 (누적: ${allData.length}개)`);
            }

            // 통합 메타데이터 생성
            combinedMetadata.totalRecords = allData.length;
            combinedMetadata.dateRange = { startDate, endDate };
            combinedMetadata.filesCount = allFiles.length;
            combinedMetadata.loadedAt = new Date().toISOString();

            console.log(`🎉 기간별 데이터 로드 완료: ${allData.length}개 레코드 (${allFiles.length}개 파일)`);

            return {
                success: true,
                metadata: combinedMetadata,
                data: allData,
                filesLoaded: allFiles.length
            };

        } catch (error) {
            console.error('기간별 데이터 로드 실패:', error.message);
            return {
                success: false,
                error: '기간별 데이터 로드 중 오류가 발생했습니다.'
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
            const response = await axios.get('https://infuser.odcloud.kr/oas/docs?namespace=15083277/v1', {
                timeout: 10000,
                headers: {
                    'Accept': 'application/json',
                    'User-Agent': 'DataCollector/1.0'
                }
            });

            if (response.data && response.data.paths) {
                const paths = response.data.paths;

                // /15083277/v1/uddi로 시작하는 path들만 필터링
                const validPaths = Object.keys(paths).filter(path =>
                    path.startsWith('/15083277/v1/uddi')
                );

                console.log(`📋 발견된 엔드포인트: ${validPaths.length}개`);

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

                            console.log(`✅ ${endpointKey}: ${getSummary.substring(0, 50)}... -> ${yearMonth}`);
                        } else {
                            console.log(`⚠️ ${path}: YYYY-MM 추출 실패 - ${getSummary.substring(0, 50)}...`);
                        }
                    }
                }

                this.uddisLoaded = true;
                console.log(`🎉 총 ${Object.keys(this.dynamicUddis).length}개 엔드포인트 로드 완료`);
            }

        } catch (error) {
            console.warn('⚠️ 동적 엔드포인트 로드 실패:', error.message);
        }

        return { ...this.uddis, ...this.dynamicUddis };
    }

    async extractYearMonthFromSummary(summary) {
        try {
            // 간단한 정규식으로 먼저 시도
            const regexMatches = [
                /(\d{4})[년\-\/\.]\s*(\d{1,2})[월\-\/\.]?/g,
                /(\d{4})\s*년\s*(\d{1,2})\s*월/g,
                /(\d{4})\-(\d{2})/g,
                /(\d{4})\.(\d{1,2})/g,
                /(\d{4})\/(\d{1,2})/g
            ];

            for (const regex of regexMatches) {
                const match = regex.exec(summary);
                if (match) {
                    const year = match[1];
                    const month = match[2].padStart(2, '0');
                    return `${year}-${month}`;
                }
            }

            // 정규식으로 찾지 못한 경우 LLM 사용
            const llmResult = await this.askLLMForYearMonth(summary);
            return llmResult;

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
            const response = await axios.get('https://infuser.odcloud.kr/oas/docs?namespace=15083277/v1', {
                timeout: 10000,
                headers: {
                    'Accept': 'application/json',
                    'User-Agent': 'DataCollector/1.0'
                }
            });

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

    async collectAllAvailableData() {
        console.log('🚀 모든 사용 가능한 엔드포인트에서 데이터 수집을 시작합니다...');

        // 동적으로 UDDI 로딩
        const allEndpoints = await this.loadDynamicUddis();

        const results = [];

        for (const [endpointName, endpointPath] of Object.entries(allEndpoints)) {
            if (endpointName === 'namespace_15083277') continue; // 베이스 패턴 스킵

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
            const status = result.success ? '✅' : '❌';
            console.log(`${status} ${result.endpoint}: ${result.recordCount.toLocaleString()}개 레코드`);
            if (result.error) {
                console.log(`   오류: ${result.error}`);
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
}

module.exports = DataCollector;