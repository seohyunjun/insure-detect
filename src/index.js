const express = require('express');
const path = require('path');
const cors = require('cors');
require('dotenv').config();

const PensionAPI = require('./api/pensionApi');
const DataProcessor = require('./data/processor');
const DataCollector = require('./services/dataCollector');
const RecentSearchService = require('./services/recentSearchService');

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
const recentSearchService = new RecentSearchService();

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

        // 사업장별 그룹화 (사업장명 + 사업자등록번호 기준)
        const workplaceGroups = {};
        rawData.forEach(item => {
            const name = item['사업장명'];
            const regNo = item['사업자등록번호'] || 'unknown';
            const key = `${name}|${regNo}`;
            if (!workplaceGroups[key]) {
                workplaceGroups[key] = {
                    사업장명: name,
                    사업자등록번호: regNo,
                    data: []
                };
            }
            workplaceGroups[key].data.push(item);
        });

        const workplaceList = Object.values(workplaceGroups);
        console.log(`- 고유 사업장 수: ${workplaceList.length}개`);
        console.log(`- 사업장별 데이터 개수:`);
        workplaceList.slice(0, 10).forEach(workplace => {
            console.log(`  ${workplace.사업장명} (${workplace.사업자등록번호}): ${workplace.data.length}개`);
        });

        // 각 사업장별로 데이터 처리
        const processingStartTime = Date.now();
        const businessResults = workplaceList.map(workplace => {
            const chartData = dataProcessor.processWorkplaceTimeSeries(workplace.data);
            const summary = dataProcessor.processWorkplaceSummary(workplace.data);
            const statistics = dataProcessor.generateStatistics(workplace.data);

            return {
                사업장명: workplace.사업장명,
                사업자등록번호: workplace.사업자등록번호,
                chartData,
                summary,
                statistics,
                rawDataCount: workplace.data.length
            };
        });
        const processingTime = ((Date.now() - processingStartTime) / 1000).toFixed(2);

        const requestEndTime = Date.now();
        const totalRequestTime = ((requestEndTime - requestStartTime) / 1000).toFixed(2);

        console.log(`\n📊 처리된 데이터 요약 (${businessResults.length}개 사업장):`);
        businessResults.forEach((business, index) => {
            console.log(`${index + 1}. ${business.사업장명} (${business.사업자등록번호})`);
            console.log(`   - 총 신규입사자: ${business.summary.totalNewHires.toLocaleString()}명`);
            console.log(`   - 총 퇴사자: ${business.summary.totalResignations.toLocaleString()}명`);
            console.log(`   - 현재 총 인원: ${business.summary.currentTotal.toLocaleString()}명`);
        });

        console.log(`\n⏱️ 처리 시간 요약:`);
        console.log(`- 데이터 로드: ${dataLoadTime}초`);
        console.log(`- 데이터 처리: ${processingTime}초`);
        console.log(`- 총 요청 시간: ${totalRequestTime}초`);
        console.log(`🎉 데이터 처리 완료: ${rawData.length}개 레코드\n`);

        res.json({
            success: true,
            data: {
                businesses: businessResults,
                totalRawDataCount: rawData.length
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

        // 기간별로 정렬 (오름차순)
        availablePeriods.sort((a, b) => a.period.localeCompare(b.period));

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
            // 실패한 검색도 기록
            await recentSearchService.addSearch({
                type: 'workplace_stats',
                startDate,
                endDate,
                workplaceName,
                resultCount: 0,
                queryTime: result.queryTime || '0',
                success: false
            });

            return res.status(404).json({
                success: false,
                error: result.error
            });
        }

        // 성공한 검색 기록 추가
        await recentSearchService.addSearch({
            type: 'workplace_stats',
            startDate,
            endDate,
            workplaceName,
            resultCount: result.recordCount || result.data?.length || 0,
            queryTime: result.queryTime || '0',
            success: true
        });

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
            // 실패한 커스텀 쿼리도 기록
            await recentSearchService.addSearch({
                type: 'custom_sql',
                startDate,
                endDate,
                customSQL: sql.substring(0, 200), // SQL 앞부분만 저장
                resultCount: 0,
                queryTime: result.queryTime || '0',
                success: false
            });

            return res.status(400).json({
                success: false,
                error: result.error
            });
        }

        // 성공한 커스텀 쿼리 기록 추가
        await recentSearchService.addSearch({
            type: 'custom_sql',
            startDate,
            endDate,
            customSQL: sql.substring(0, 200), // SQL 앞부분만 저장
            resultCount: result.recordCount || result.data?.length || 0,
            queryTime: result.queryTime || '0',
            success: true
        });

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

// 📝 최근 검색 내역 관련 API 엔드포인트들

// 최근 검색 내역 조회
app.get('/api/recent-searches', async (req, res) => {
    try {
        const { limit = 20, type } = req.query;

        let searches;
        if (type) {
            searches = recentSearchService.getSearchesByType(type, parseInt(limit));
        } else {
            searches = recentSearchService.getRecentSearches(parseInt(limit));
        }

        res.json({
            success: true,
            data: searches,
            total: searches.length
        });

    } catch (error) {
        console.error('최근 검색 내역 조회 오류:', error);
        res.status(500).json({
            success: false,
            error: '서버 내부 오류가 발생했습니다.'
        });
    }
});

// 검색 내역 요약 정보
app.get('/api/recent-searches/summary', async (req, res) => {
    try {
        const summary = recentSearchService.getSearchSummary();

        res.json({
            success: true,
            data: summary
        });

    } catch (error) {
        console.error('검색 내역 요약 조회 오류:', error);
        res.status(500).json({
            success: false,
            error: '서버 내부 오류가 발생했습니다.'
        });
    }
});

// 인기 검색 조건 분석
app.get('/api/recent-searches/popular', async (req, res) => {
    try {
        const popularSearches = recentSearchService.getPopularSearches();

        res.json({
            success: true,
            data: popularSearches
        });

    } catch (error) {
        console.error('인기 검색 분석 오류:', error);
        res.status(500).json({
            success: false,
            error: '서버 내부 오류가 발생했습니다.'
        });
    }
});

// 특정 검색 내역 삭제
app.delete('/api/recent-searches/:searchId', async (req, res) => {
    try {
        const { searchId } = req.params;

        const deleted = await recentSearchService.deleteSearch(searchId);

        if (deleted) {
            res.json({
                success: true,
                message: '검색 내역이 삭제되었습니다.'
            });
        } else {
            res.status(404).json({
                success: false,
                error: '검색 내역을 찾을 수 없습니다.'
            });
        }

    } catch (error) {
        console.error('검색 내역 삭제 오류:', error);
        res.status(500).json({
            success: false,
            error: '서버 내부 오류가 발생했습니다.'
        });
    }
});

// 모든 검색 내역 삭제
app.delete('/api/recent-searches', async (req, res) => {
    try {
        await recentSearchService.clearAllSearches();

        res.json({
            success: true,
            message: '모든 검색 내역이 삭제되었습니다.'
        });

    } catch (error) {
        console.error('모든 검색 내역 삭제 오류:', error);
        res.status(500).json({
            success: false,
            error: '서버 내부 오류가 발생했습니다.'
        });
    }
});

// 🗺️ VWorld 지오코딩 API 프록시 엔드포인트
app.get('/api/geocode', async (req, res) => {
    try {
        const { address } = req.query;

        if (!address || address.trim().length < 2) {
            return res.status(400).json({
                success: false,
                error: '주소를 2글자 이상 입력해주세요.'
            });
        }

        const vworldApiKey = process.env.VWORLD_API_KEY;
        if (!vworldApiKey || vworldApiKey === 'your_vworld_api_key_here') {
            return res.status(500).json({
                success: false,
                error: 'VWorld API 키가 설정되지 않았습니다. .env 파일에서 VWORLD_API_KEY를 설정해주세요.'
            });
        }

        console.log(`🗺️ 지오코딩 요청: ${address}`);

        const axios = require('axios');
        
        // VWorld Geocoder API 호출 (도로명주소 검색)
        const encodedAddress = encodeURIComponent(address.trim());
        const apiUrl = `https://api.vworld.kr/req/address?service=address&request=getcoord&version=2.0&crs=epsg:4326&address=${encodedAddress}&format=json&type=road&key=${vworldApiKey}`;

        const response = await axios.get(apiUrl, {
            timeout: 10000,
            headers: {
                'Accept': 'application/json'
            }
        });

        const result = response.data;

        if (result.response && result.response.status === 'OK' && result.response.result) {
            const point = result.response.result.point;
            
            console.log(`✅ 지오코딩 성공: ${address} → (${point.y}, ${point.x})`);
            
            res.json({
                success: true,
                data: {
                    address: address,
                    lat: parseFloat(point.y),
                    lng: parseFloat(point.x),
                    fullAddress: result.response.refined?.text || address
                }
            });
        } else {
            // 도로명주소로 검색 실패시 지번주소로 재시도
            const parcelApiUrl = `https://api.vworld.kr/req/address?service=address&request=getcoord&version=2.0&crs=epsg:4326&address=${encodedAddress}&format=json&type=parcel&key=${vworldApiKey}`;
            
            const parcelResponse = await axios.get(parcelApiUrl, {
                timeout: 10000,
                headers: {
                    'Accept': 'application/json'
                }
            });

            const parcelResult = parcelResponse.data;

            if (parcelResult.response && parcelResult.response.status === 'OK' && parcelResult.response.result) {
                const point = parcelResult.response.result.point;
                
                console.log(`✅ 지오코딩 성공 (지번): ${address} → (${point.y}, ${point.x})`);
                
                res.json({
                    success: true,
                    data: {
                        address: address,
                        lat: parseFloat(point.y),
                        lng: parseFloat(point.x),
                        fullAddress: parcelResult.response.refined?.text || address
                    }
                });
            } else {
                console.log(`⚠️ 지오코딩 실패: ${address} - 주소를 찾을 수 없음`);
                res.json({
                    success: false,
                    error: '해당 주소의 좌표를 찾을 수 없습니다.'
                });
            }
        }
    } catch (error) {
        console.error('지오코딩 API 오류:', error.message);
        res.status(500).json({
            success: false,
            error: '지오코딩 중 오류가 발생했습니다.'
        });
    }
});

// 🗺️ 사업장 위치 조회 API (주소로 좌표 변환 포함)
app.post('/api/workplace-location', async (req, res) => {
    try {
        const { workplaceName, startDate, endDate } = req.body;

        if (!workplaceName) {
            return res.status(400).json({
                success: false,
                error: '사업장명이 필요합니다.'
            });
        }

        console.log(`🗺️ 사업장 위치 조회 요청: ${workplaceName}`);

        // 사업장 데이터 조회 (최신 데이터 기준)
        const result = await dataCollector.queryDataByDateRange(
            startDate || '2025-11',
            endDate || '2025-11',
            'pension_workplace',
            workplaceName
        );

        if (!result.success || !result.data || result.data.length === 0) {
            return res.json({
                success: false,
                error: '해당 사업장을 찾을 수 없습니다.'
            });
        }

        // 사업장별로 그룹화하여 주소 정보 추출 (가장 최근 날짜의 가입자수 사용)
        const workplaceMap = new Map();
        
        result.data.forEach(item => {
            const key = `${item['사업장명']}|${item['사업자등록번호']}`;
            const itemDate = item['자료생성년월'] || '';
            
            if (!workplaceMap.has(key)) {
                workplaceMap.set(key, {
                    name: item['사업장명'],
                    regNo: item['사업자등록번호'],
                    roadAddress: item['사업장도로명상세주소'] || '',
                    parcelAddress: item['사업장지번상세주소'] || '',
                    zipCode: item['우편번호'] || '',
                    memberCount: parseInt(item['가입자수']) || 0,
                    industry: item['사업장업종코드명'] || '',
                    latestDate: itemDate
                });
            } else {
                // 기존 데이터보다 최신 날짜인 경우 가입자수 업데이트
                const existing = workplaceMap.get(key);
                if (itemDate > existing.latestDate) {
                    existing.memberCount = parseInt(item['가입자수']) || 0;
                    existing.latestDate = itemDate;
                    // 주소 정보도 최신 데이터로 업데이트
                    if (item['사업장도로명상세주소']) {
                        existing.roadAddress = item['사업장도로명상세주소'];
                    }
                    if (item['사업장지번상세주소']) {
                        existing.parcelAddress = item['사업장지번상세주소'];
                    }
                    if (item['사업장업종코드명']) {
                        existing.industry = item['사업장업종코드명'];
                    }
                }
            }
        });

        const workplaces = Array.from(workplaceMap.values());
        
        // 각 사업장의 좌표 조회
        const axios = require('axios');
        const vworldApiKey = process.env.VWORLD_API_KEY;
        
        const locatedWorkplaces = [];
        
        for (const workplace of workplaces.slice(0, 10)) { // 최대 10개까지만 처리
            const address = workplace.roadAddress || workplace.parcelAddress;
            
            if (!address || address.trim().length < 2) {
                locatedWorkplaces.push({
                    ...workplace,
                    lat: null,
                    lng: null,
                    geocodeError: '주소 정보가 없습니다.'
                });
                continue;
            }

            if (!vworldApiKey || vworldApiKey === 'your_vworld_api_key_here') {
                locatedWorkplaces.push({
                    ...workplace,
                    lat: null,
                    lng: null,
                    geocodeError: 'VWorld API 키가 설정되지 않았습니다.'
                });
                continue;
            }

            try {
                // VWorld API로 좌표 조회
                const encodedAddress = encodeURIComponent(address.trim());
                const addressType = workplace.roadAddress ? 'road' : 'parcel';
                const apiUrl = `https://api.vworld.kr/req/address?service=address&request=getcoord&version=2.0&crs=epsg:4326&address=${encodedAddress}&format=json&type=${addressType}&key=${vworldApiKey}`;

                const response = await axios.get(apiUrl, { timeout: 5000 });
                const geoResult = response.data;

                if (geoResult.response && geoResult.response.status === 'OK' && geoResult.response.result) {
                    const point = geoResult.response.result.point;
                    locatedWorkplaces.push({
                        ...workplace,
                        lat: parseFloat(point.y),
                        lng: parseFloat(point.x)
                    });
                } else {
                    // 다른 타입으로 재시도
                    const altType = addressType === 'road' ? 'parcel' : 'road';
                    const altApiUrl = `https://api.vworld.kr/req/address?service=address&request=getcoord&version=2.0&crs=epsg:4326&address=${encodedAddress}&format=json&type=${altType}&key=${vworldApiKey}`;
                    
                    const altResponse = await axios.get(altApiUrl, { timeout: 5000 });
                    const altResult = altResponse.data;
                    
                    if (altResult.response && altResult.response.status === 'OK' && altResult.response.result) {
                        const point = altResult.response.result.point;
                        locatedWorkplaces.push({
                            ...workplace,
                            lat: parseFloat(point.y),
                            lng: parseFloat(point.x)
                        });
                    } else {
                        locatedWorkplaces.push({
                            ...workplace,
                            lat: null,
                            lng: null,
                            geocodeError: '좌표 변환 실패'
                        });
                    }
                }
            } catch (geoError) {
                console.error(`지오코딩 오류 (${workplace.name}):`, geoError.message);
                locatedWorkplaces.push({
                    ...workplace,
                    lat: null,
                    lng: null,
                    geocodeError: geoError.message
                });
            }
        }

        console.log(`✅ 사업장 위치 조회 완료: ${locatedWorkplaces.length}개`);

        res.json({
            success: true,
            data: locatedWorkplaces
        });

    } catch (error) {
        console.error('사업장 위치 조회 오류:', error);
        res.status(500).json({
            success: false,
            error: '서버 내부 오류가 발생했습니다.'
        });
    }
});

// 🏢 사업장명 제안 API 엔드포인트
app.get('/api/workplace-suggestions', async (req, res) => {
    try {
        // 인기 사업장 리스트 (실제로는 최근 검색이나 데이터에서 가져올 수 있음)
        const popularWorkplaces = [
            '삼성전자',
            '현대자동차',
            '엘지전자',
            'SK하이닉스',
            '포스코',
            '롯데',
            '현대건설',
            '대한항공',
            '국민은행',
            '우리은행',
            '신한은행',
            '하나은행'
        ];

        // 최근 검색에서 인기 사업장명 가져오기
        const recentSearches = recentSearchService.getRecentSearches(50);
        const workplaceFrequency = {};

        // 최근 검색에서 사업장명 추출하여 빈도 계산
        recentSearches.forEach(search => {
            if (search.parameters.workplaceName) {
                const name = search.parameters.workplaceName;
                workplaceFrequency[name] = (workplaceFrequency[name] || 0) + 1;
            }
        });

        // 빈도순으로 정렬하여 상위 5개 추출
        const recentPopular = Object.entries(workplaceFrequency)
            .sort(([,a], [,b]) => b - a)
            .slice(0, 5)
            .map(([name]) => name);

        // 최근 인기 검색과 기본 인기 사업장 합치기 (중복 제거)
        const suggestions = [...new Set([...recentPopular, ...popularWorkplaces])].slice(0, 12);

        res.json({
            success: true,
            data: {
                suggestions: suggestions,
                recentPopular: recentPopular,
                defaultSuggestions: popularWorkplaces.slice(0, 8)
            }
        });

    } catch (error) {
        console.error('사업장명 제안 조회 오류:', error);
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