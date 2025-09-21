const DataCollector = require('../services/dataCollector');
require('dotenv').config();

class PensionAPI {
    constructor() {
        this.dataCollector = new DataCollector();
        this.localData = null;
        this.lastLoadTime = null;
    }

    async loadLocalData() {
        try {
            if (!this.localData || this.isDataStale()) {
                console.log('📂 로컬 데이터를 로드합니다...');
                const result = await this.dataCollector.loadData('pension_workplace');

                if (result.success) {
                    this.localData = result.data;
                    this.lastLoadTime = new Date();
                    console.log(`✅ 로컬 데이터 로드 완료: ${this.localData.length}개 레코드`);
                } else {
                    console.error('❌ 로컬 데이터 로드 실패:', result.error);
                    throw new Error(result.error);
                }
            }
            return this.localData;
        } catch (error) {
            console.error('로컬 데이터 로드 오류:', error.message);
            throw error;
        }
    }

    isDataStale() {
        if (!this.lastLoadTime) return true;
        const staleTime = 30 * 60 * 1000; // 30분
        return (new Date() - this.lastLoadTime) > staleTime;
    }

    async fetchWorkplaceData(params = {}) {
        const {
            stdrYm = '', // 기준년월 (YYYYMM)
            bizplcNm = '', // 사업장명
            numOfRows = 100, // 한 페이지 결과 수
            pageNo = 1 // 페이지번호
        } = params;

        try {
            // 로컬 데이터 로드
            const allData = await this.loadLocalData();
            let filteredData = [...allData];

            // 기준년월 필터링
            if (stdrYm) {
                filteredData = filteredData.filter(item =>
                    item['자료생성년월'] && item['자료생성년월'].includes(stdrYm)
                );
            }

            // 사업장명 필터링 (부분 매칭)
            if (bizplcNm) {
                filteredData = filteredData.filter(item =>
                    item['사업장명'] &&
                    item['사업장명'].toLowerCase().includes(bizplcNm.toLowerCase())
                );
            }

            // 페이징 처리
            const totalCount = filteredData.length;
            const startIndex = (pageNo - 1) * numOfRows;
            const endIndex = startIndex + numOfRows;
            const pageData = filteredData.slice(startIndex, endIndex);

            return {
                success: true,
                data: pageData,
                totalCount: totalCount,
                currentCount: pageData.length,
                page: pageNo,
                perPage: numOfRows
            };
        } catch (error) {
            console.error('데이터 조회 오류:', error.message);
            return {
                success: false,
                error: error.message,
                data: []
            };
        }
    }

    async fetchWorkplaceDataByPeriod(bizplcNm, startYm, endYm) {
        try {
            // 로컬 데이터 로드
            const allData = await this.loadLocalData();
            let filteredData = [...allData];

            // 사업장명 필터링 (부분 매칭)
            if (bizplcNm) {
                filteredData = filteredData.filter(item =>
                    item['사업장명'] &&
                    item['사업장명'].toLowerCase().includes(bizplcNm.toLowerCase())
                );
            }

            // 기간 필터링
            if (startYm && endYm) {
                filteredData = filteredData.filter(item => {
                    const itemYm = item['자료생성년월'];
                    if (!itemYm) return false;

                    const itemDate = parseInt(itemYm);
                    const startDate = parseInt(startYm);
                    const endDate = parseInt(endYm);

                    return itemDate >= startDate && itemDate <= endDate;
                });
            }

            console.log(`📊 기간별 데이터 필터링 결과: ${filteredData.length}개 레코드`);

            return filteredData;
        } catch (error) {
            console.error('기간별 데이터 조회 오류:', error.message);
            return [];
        }
    }

    async searchWorkplaces(workplaceName, limit = 50) {
        try {
            const result = await this.fetchWorkplaceData({
                bizplcNm: workplaceName,
                numOfRows: limit * 5 // 중복 제거를 고려해 더 많이 가져옴
            });

            if (result.success) {
                // 사업장명으로 그룹화하여 중복 제거
                const workplaces = {};
                result.data.forEach(item => {
                    if (item['사업장명'] && !workplaces[item['사업장명']]) {
                        workplaces[item['사업장명']] = {
                            name: item['사업장명'],
                            latestData: item
                        };
                    }
                });

                // 제한된 수만 반환
                return Object.values(workplaces).slice(0, limit);
            }

            return [];
        } catch (error) {
            console.error('사업장 검색 오류:', error.message);
            return [];
        }
    }

    // 데이터 수집을 위한 메서드 추가
    async collectAllData() {
        return await this.dataCollector.collectAllData('pension_workplace');
    }

    // 사용 가능한 데이터 조회
    async getAvailableData() {
        return await this.dataCollector.getAvailableData();
    }

    // 오래된 파일 정리
    async cleanupOldFiles(daysToKeep = 30) {
        return await this.dataCollector.cleanupOldFiles(daysToKeep);
    }

    // 모든 사용 가능한 엔드포인트 조회
    async getAllAvailableEndpoints() {
        return await this.dataCollector.getAllAvailableEndpoints();
    }

    // 모든 엔드포인트에서 데이터 수집
    async collectAllAvailableData() {
        return await this.dataCollector.collectAllAvailableData();
    }
}

module.exports = PensionAPI;