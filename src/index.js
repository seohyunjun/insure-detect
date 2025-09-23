const express = require('express');
const path = require('path');
const cors = require('cors');
require('dotenv').config();

const PensionAPI = require('./api/pensionApi');
const DataProcessor = require('./data/processor');
const DataCollector = require('./services/dataCollector');

const app = express();
const PORT = process.env.PORT || 3000;

// 미들웨어 설정
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

// 인스턴스 생성
const pensionAPI = new PensionAPI();
const dataProcessor = new DataProcessor();
const dataCollector = new DataCollector();

// 메인 페이지 라우트
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '../public/index.html'));
});

// API 라우트: 데이터 수집
app.post('/api/collect-data', async (req, res) => {
    try {
        console.log('🚀 데이터 수집 요청 시작...');

        const result = await pensionAPI.collectAllData();

        if (result.success) {
            res.json({
                success: true,
                message: '데이터 수집이 완료되었습니다.',
                data: {
                    recordCount: result.recordCount,
                    dataFile: result.dataFile,
                    latestFile: result.latestFile,
                    collectedAt: result.metadata.collectedAt
                }
            });
        } else {
            res.status(500).json({
                success: false,
                error: '데이터 수집 중 오류가 발생했습니다.'
            });
        }
    } catch (error) {
        console.error('데이터 수집 오류:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// API 라우트: 사용 가능한 데이터 조회
app.get('/api/available-data', async (req, res) => {
    try {
        const availableData = await pensionAPI.getAvailableData();
        res.json({
            success: true,
            data: availableData
        });
    } catch (error) {
        console.error('사용 가능한 데이터 조회 오류:', error);
        res.status(500).json({
            success: false,
            error: '데이터 조회 중 오류가 발생했습니다.'
        });
    }
});

// API 라우트: 사업장 데이터 조회
app.post('/api/workplace-data', async (req, res) => {
    const requestStartTime = Date.now();
    try {
        const { workplaceName, startDate, endDate } = req.body;

        if (!workplaceName || !startDate || !endDate) {
            return res.status(400).json({
                success: false,
                error: '사업장명, 시작날짜, 종료날짜가 모두 필요합니다.'
            });
        }

        console.log(`⏱️ API 요청 시작: ${workplaceName} (${startDate} ~ ${endDate})`);

        // 🦆 DuckDB SQL 질의로 로컬 데이터에서 기간별로 모든 파일 로드 (사업장명 필터링 포함)
        const dataLoadStartTime = Date.now();
        const result = await dataCollector.queryDataByDateRange(startDate, endDate, 'pension_workplace', workplaceName);
        const dataLoadTime = ((Date.now() - dataLoadStartTime) / 1000).toFixed(2);

        if (!result.success) {
            return res.status(404).json({
                success: false,
                error: result.error
            });
        }

        let rawData = result.data;
        console.log(`✅ 로컬 데이터 로드 완료: ${rawData.length}개 레코드 (${result.metadata.totalProcessedRecords || 0}개 중 필터링, ${result.filesLoaded || 1}개 파일, ${dataLoadTime}초)`);

        // 추가 기간 필터링 (파일 기반 로드에서 누락된 부분 처리)
        const filterStartTime = Date.now();
        if (startDate && endDate) {
            const beforeFilter = rawData.length;
            rawData = dataProcessor.filterDataByDateRange(rawData, startDate, endDate);
            const filterTime = ((Date.now() - filterStartTime) / 1000).toFixed(2);
            console.log(`📊 기간별 데이터 필터링 결과: ${beforeFilter}개 → ${rawData.length}개 레코드 (${filterTime}초)`);
        }

        // 이미 스트리밍 중에 사업장명 필터링이 완료되었으므로 추가 필터링 불필요
        if (rawData && rawData.length > 0) {
            console.log(`🎯 매칭된 사업장들 (상위 20개):`);
            const uniqueNames = [...new Set(rawData.map(item => item['사업장명']))];
            uniqueNames.slice(0, 20).forEach(name => {
                console.log(`  - ${name}`);
            });
            if (uniqueNames.length > 20) {
                console.log(`  - ... 외 ${uniqueNames.length - 20}개`);
            }
        }

        if (!rawData || rawData.length === 0) {
            return res.json({
                success: false,
                error: '해당 기간에 대한 데이터를 찾을 수 없습니다. 사업장명과 기간을 확인해주세요.'
            });
        }

        console.log(`\n🔍 디버깅 정보 - ${workplaceName}:`);
        console.log(`- 총 데이터 개수: ${rawData.length}개`);

        // 처음 5개 데이터 샘플 출력
        console.log(`- 처음 5개 데이터 샘플:`);
        rawData.slice(0, 5).forEach((item, index) => {
            console.log(`  ${index + 1}. ${item['사업장명']} (${item['자료생성년월']})`);
            console.log(`     - 신규취득자수: ${item['신규취득자수']}`);
            console.log(`     - 상실가입자수: ${item['상실가입자수']}`);
            console.log(`     - 가입자수: ${item['가입자수']}`);
        });

        // 사업장명별 그룹화 확인
        const workplaceGroups = {};
        rawData.forEach(item => {
            const name = item['사업장명'];
            if (!workplaceGroups[name]) {
                workplaceGroups[name] = [];
            }
            workplaceGroups[name].push(item);
        });

        console.log(`- 고유 사업장 수: ${Object.keys(workplaceGroups).length}개`);
        console.log(`- 사업장별 데이터 개수:`);
        Object.entries(workplaceGroups).slice(0, 10).forEach(([name, data]) => {
            console.log(`  ${name}: ${data.length}개`);
        });

        // 데이터 처리
        const processingStartTime = Date.now();
        const chartData = dataProcessor.processWorkplaceTimeSeries(rawData);
        const summary = dataProcessor.processWorkplaceSummary(rawData);
        const statistics = dataProcessor.generateStatistics(rawData);
        const processingTime = ((Date.now() - processingStartTime) / 1000).toFixed(2);

        const requestEndTime = Date.now();
        const totalRequestTime = ((requestEndTime - requestStartTime) / 1000).toFixed(2);

        console.log(`\n📊 처리된 데이터 요약:`);
        console.log(`- 총 신규입사자: ${summary.totalNewHires.toLocaleString()}명`);
        console.log(`- 총 퇴사자: ${summary.totalResignations.toLocaleString()}명`);
        console.log(`- 현재 총 인원: ${summary.currentTotal.toLocaleString()}명`);
        console.log(`- 월평균 변화: ${summary.averageMonthlyChange}명`);

        console.log(`\n⏱️ 처리 시간 요약:`);
        console.log(`- 데이터 로드: ${dataLoadTime}초`);
        console.log(`- 데이터 처리: ${processingTime}초`);
        console.log(`- 총 요청 시간: ${totalRequestTime}초`);
        console.log(`🎉 데이터 처리 완료: ${rawData.length}개 레코드\n`);

        res.json({
            success: true,
            data: {
                chartData,
                summary,
                statistics,
                rawDataCount: rawData.length
            }
        });

    } catch (error) {
        console.error('사업장 데이터 조회 오류:', error);
        res.status(500).json({
            success: false,
            error: '서버 내부 오류가 발생했습니다.'
        });
    }
});

// API 라우트: 사업장 비교
app.post('/api/compare-workplaces', async (req, res) => {
    try {
        const { workplaceNames, startDate, endDate } = req.body;

        if (!workplaceNames || !Array.isArray(workplaceNames) || workplaceNames.length < 2) {
            return res.status(400).json({
                success: false,
                error: '비교할 사업장을 2개 이상 입력해주세요.'
            });
        }

        if (!startDate || !endDate) {
            return res.status(400).json({
                success: false,
                error: '시작날짜와 종료날짜가 필요합니다.'
            });
        }

        console.log(`사업장 비교 요청: ${workplaceNames.join(', ')} (${startDate} ~ ${endDate})`);

        const startYm = startDate.replace('-', '');
        const endYm = endDate.replace('-', '');

        const workplacesData = {};

        // 각 사업장별로 데이터 수집
        for (const workplaceName of workplaceNames) {
            try {
                const rawData = await pensionAPI.fetchWorkplaceDataByPeriod(
                    workplaceName.trim(),
                    startYm,
                    endYm
                );

                if (rawData && rawData.length > 0) {
                    workplacesData[workplaceName] = rawData;
                }
            } catch (error) {
                console.error(`${workplaceName} 데이터 수집 실패:`, error.message);
            }
        }

        if (Object.keys(workplacesData).length === 0) {
            return res.json({
                success: false,
                error: '비교할 사업장의 데이터를 찾을 수 없습니다.'
            });
        }

        // 비교 데이터 처리
        const comparisonData = dataProcessor.processWorkplaceComparison(workplacesData);

        console.log(`비교 데이터 처리 완료: ${comparisonData.length}개 사업장`);

        res.json({
            success: true,
            data: comparisonData
        });

    } catch (error) {
        console.error('사업장 비교 오류:', error);
        res.status(500).json({
            success: false,
            error: '서버 내부 오류가 발생했습니다.'
        });
    }
});

// API 라우트: 사업장 검색
app.get('/api/search-workplaces', async (req, res) => {
    try {
        const { q: query, limit = 20 } = req.query;

        if (!query || query.trim().length < 2) {
            return res.status(400).json({
                success: false,
                error: '검색어는 2글자 이상이어야 합니다.'
            });
        }

        console.log(`사업장 검색: ${query}`);

        const workplaces = await pensionAPI.searchWorkplaces(query.trim(), parseInt(limit));

        res.json({
            success: true,
            data: workplaces
        });

    } catch (error) {
        console.error('사업장 검색 오류:', error);
        res.status(500).json({
            success: false,
            error: '검색 중 오류가 발생했습니다.'
        });
    }
});


// 상태 확인 라우트
app.get('/api/health', (req, res) => {
    res.json({
        success: true,
        status: 'OK',
        timestamp: new Date().toISOString(),
        env: {
            nodeEnv: process.env.NODE_ENV || 'development',
            port: PORT,
            hasApiKey: !!process.env.API_KEY
        }
    });
});

// 사용 가능한 데이터 기간 조회 엔드포인트
app.get('/api/available-periods', async (req, res) => {
    try {
        const fs = require('fs').promises;
        const path = require('path');
        const sourceDir = path.join(__dirname, '../source/data');

        const files = await fs.readdir(sourceDir);
        const availablePeriods = [];

        // 파일에서 기간 정보 추출
        for (const file of files) {
            if (file.endsWith('.parquet')) {
                let period = null;
                let fileType = null;

                if (file.startsWith('pension_workplace_')) {
                    const match = file.match(/pension_workplace_(\d{4}-\d{2})\.parquet$/);
                    if (match) {
                        period = match[1];
                        fileType = 'latest';
                    }
                } else if (file.startsWith('pension_')) {
                    const match = file.match(/pension_(\d{4}-\d{2})_(\d{4}-\d{2})\.parquet$/);
                    if (match) {
                        // 실제 데이터 기간은 두 번째 날짜
                        period = match[2];
                        fileType = 'archive';
                    }
                }

                if (period) {
                    availablePeriods.push({
                        period,
                        fileName: file,
                        type: fileType
                    });
                }
            }
        }

        // 기간별로 정렬
        availablePeriods.sort((a, b) => b.period.localeCompare(a.period));

        res.json({
            success: true,
            periods: availablePeriods,
            count: availablePeriods.length
        });
    } catch (error) {
        console.error('사용 가능한 기간 조회 실패:', error);
        res.status(500).json({ error: error.message });
    }
});

// 데이터 샘플 확인 엔드포인트 (디버깅용)
app.get('/api/debug/sample', async (req, res) => {
    try {
        const { startDate, endDate } = req.query;

        let result;
        if (startDate && endDate) {
            // 🦆 DuckDB SQL 질의 기간별 데이터 로드
            result = await dataCollector.queryDataByDateRange(startDate, endDate);
        } else {
            // 🚀 고성능 기본 데이터 로드
            result = await dataCollector.loadDataFast();
        }

        if (!result.success) {
            return res.status(404).json({ error: result.error });
        }

        const sampleData = result.data.slice(0, 5); // 첫 5개 레코드
        const dateValues = result.data.slice(0, 100).map(item => item['자료생성년월']).filter(Boolean);
        const uniqueDates = [...new Set(dateValues)].slice(0, 20);

        res.json({
            totalRecords: result.data.length,
            sampleData: sampleData,
            dateFormats: uniqueDates,
            metadata: result.metadata,
            filesLoaded: result.filesLoaded || 1
        });
    } catch (error) {
        console.error('샘플 데이터 조회 실패:', error);
        res.status(500).json({ error: error.message });
    }
});

// 404 에러 처리
// 🦆 사업장별 통계 질의 API
app.get('/api/workplace-stats', async (req, res) => {
    try {
        const { startDate, endDate, workplaceName } = req.query;

        if (!startDate || !endDate) {
            return res.status(400).json({
                success: false,
                error: 'startDate와 endDate 파라미터가 필요합니다.'
            });
        }

        console.log(`📊 사업장 통계 API 요청: ${workplaceName || '전체'} (${startDate} ~ ${endDate})`);

        const result = await dataCollector.getWorkplaceStatistics(startDate, endDate, workplaceName);

        if (!result.success) {
            return res.status(404).json({
                success: false,
                error: result.error
            });
        }

        res.json({
            success: true,
            data: result.data,
            queryTime: result.queryTime,
            recordCount: result.recordCount
        });

    } catch (error) {
        console.error('사업장 통계 API 오류:', error);
        res.status(500).json({
            success: false,
            error: '서버 내부 오류가 발생했습니다.'
        });
    }
});

// 🔧 커스텀 SQL 질의 API (고급 사용자용)
app.post('/api/custom-query', async (req, res) => {
    try {
        const { sql, startDate, endDate } = req.body;

        if (!sql) {
            return res.status(400).json({
                success: false,
                error: 'SQL 질의가 필요합니다.'
            });
        }

        console.log(`🔧 커스텀 SQL API 요청: ${sql.substring(0, 100)}...`);

        const result = await dataCollector.executeCustomSQL(sql, startDate, endDate);

        if (!result.success) {
            return res.status(400).json({
                success: false,
                error: result.error
            });
        }

        res.json({
            success: true,
            data: result.data,
            queryTime: result.queryTime,
            recordCount: result.recordCount
        });

    } catch (error) {
        console.error('커스텀 SQL API 오류:', error);
        res.status(500).json({
            success: false,
            error: '서버 내부 오류가 발생했습니다.'
        });
    }
});

app.use('*', (req, res) => {
    res.status(404).json({
        success: false,
        error: '요청한 경로를 찾을 수 없습니다.'
    });
});

// 전역 에러 처리
app.use((error, req, res, next) => {
    console.error('전역 에러:', error);
    res.status(500).json({
        success: false,
        error: '서버 내부 오류가 발생했습니다.'
    });
});

// 서버 시작
app.listen(PORT, () => {
    console.log(`🚀 서버가 포트 ${PORT}에서 실행 중입니다.`);
    console.log(`📊 웹 인터페이스: http://localhost:${PORT}`);
    console.log(`🦆 DuckDB SQL 질의 기능이 활성화되었습니다.`);
    console.log(`🔧 API 상태 확인: http://localhost:${PORT}/api/health`);

    // API 키 확인
    if (!process.env.API_KEY || process.env.API_KEY === 'your_api_key_here') {
        console.log('⚠️  경고: API 키가 설정되지 않았습니다. .env 파일을 확인해주세요.');
    } else {
        console.log('✅ API 키가 설정되었습니다.');
    }
});

// 프로세스 종료 시 정리
process.on('SIGTERM', () => {
    console.log('서버를 종료합니다...');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('서버를 종료합니다...');
    process.exit(0);
});

module.exports = app;