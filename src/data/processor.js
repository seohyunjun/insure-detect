const moment = require('moment');

// 국민연금 기준소득월액 상한액/하한액 연도별 테이블
// 매년 7월부터 다음해 6월까지 적용
const PENSION_INCOME_LIMITS = {
    // 적용시작: { 상한액, 하한액, 보험료율(%) }
    '2015-07': { upper: 4210000, lower: 280000, rate: 9 },
    '2016-07': { upper: 4340000, lower: 290000, rate: 9 },
    '2017-07': { upper: 4490000, lower: 300000, rate: 9 },
    '2018-07': { upper: 4680000, lower: 310000, rate: 9 },
    '2019-07': { upper: 4860000, lower: 320000, rate: 9 },
    '2020-07': { upper: 5030000, lower: 330000, rate: 9 },
    '2021-07': { upper: 5240000, lower: 330000, rate: 9 },
    '2022-07': { upper: 5530000, lower: 350000, rate: 9 },
    '2023-07': { upper: 5900000, lower: 370000, rate: 9 },
    '2024-07': { upper: 6170000, lower: 390000, rate: 9 },
    '2025-07': { upper: 6370000, lower: 400000, rate: 9 },
};

/**
 * 주어진 년월에 해당하는 국민연금 기준소득월액 상한액/하한액/요율 반환
 * @param {string} yearMonth - 'YYYYMM' 또는 'YYYY-MM' 형식
 * @returns {{ upper: number, lower: number, rate: number }}
 */
function getPensionLimitsForMonth(yearMonth) {
    // YYYYMM 형식을 YYYY-MM으로 변환
    let normalizedMonth;
    if (yearMonth.length === 6) {
        normalizedMonth = `${yearMonth.substring(0, 4)}-${yearMonth.substring(4, 6)}`;
    } else {
        normalizedMonth = yearMonth;
    }
    
    const year = parseInt(normalizedMonth.substring(0, 4));
    const month = parseInt(normalizedMonth.substring(5, 7));
    
    // 적용 기준년월 계산 (7월부터 다음해 6월까지)
    let effectiveYear;
    if (month >= 7) {
        effectiveYear = year;
    } else {
        effectiveYear = year - 1;
    }
    
    const effectiveKey = `${effectiveYear}-07`;
    
    // 해당 연도의 설정이 있으면 사용, 없으면 가장 가까운 과거 설정 사용
    if (PENSION_INCOME_LIMITS[effectiveKey]) {
        return PENSION_INCOME_LIMITS[effectiveKey];
    }
    
    // 가장 가까운 과거 설정 찾기
    const sortedKeys = Object.keys(PENSION_INCOME_LIMITS).sort();
    let closestKey = sortedKeys[0]; // 기본값: 가장 오래된 설정
    
    for (const key of sortedKeys) {
        if (key <= effectiveKey) {
            closestKey = key;
        } else {
            break;
        }
    }
    
    return PENSION_INCOME_LIMITS[closestKey];
}

/**
 * 국민연금 고지금액으로부터 추정 기준소득월액 계산
 * @param {number} pensionAmount - 1인당 월 국민연금 고지금액 (사업장+개인 합계)
 * @param {string} yearMonth - 'YYYYMM' 또는 'YYYY-MM' 형식
 * @returns {{ estimatedIncome: number, isAtUpperLimit: boolean, isAtLowerLimit: boolean, limits: object }}
 */
function estimateMonthlyIncome(pensionAmount, yearMonth) {
    const limits = getPensionLimitsForMonth(yearMonth);
    
    // 기준소득월액 역산: 고지금액 / 요율 * 100
    const estimatedIncome = Math.round((pensionAmount / limits.rate) * 100);
    
    // 상한액/하한액에 도달했는지 확인
    const upperLimitPension = Math.round(limits.upper * limits.rate / 100);
    const lowerLimitPension = Math.round(limits.lower * limits.rate / 100);
    
    const isAtUpperLimit = pensionAmount >= upperLimitPension * 0.98; // 상한액의 98% 이상
    const isAtLowerLimit = pensionAmount <= lowerLimitPension * 1.02; // 하한액의 102% 이하
    
    return {
        estimatedIncome,
        isAtUpperLimit,
        isAtLowerLimit,
        limits
    };
}

// 컬럼명 정리 및 타입 정보 분리
function cleanColumnName(dirtyColumnName) {
    // 한글 컬럼명만 추출 (첫 번째 공백 전까지)
    const cleanName = dirtyColumnName.split(' ')[0];
    return cleanName;
}

function extractColumnType(dirtyColumnName) {
    // 타입 정보 추출 (VARCHAR, INTEGER 등)
    const typeMatch = dirtyColumnName.match(/(VARCHAR\(\d+\)|INTEGER|VARCHAR)/);
    return typeMatch ? typeMatch[1] : 'UNKNOWN';
}

function cleanDataObject(rawDataObject) {
    const cleanedData = {};
    const columnTypes = {};

    for (const [key, value] of Object.entries(rawDataObject)) {
        const cleanKey = cleanColumnName(key);
        const columnType = extractColumnType(key);

        cleanedData[cleanKey] = value;
        columnTypes[cleanKey] = columnType;
    }

    return { data: cleanedData, types: columnTypes };
}

class DataProcessor {
    constructor() {
        // 데이터 처리에 필요한 초기 설정
    }

    processWorkplaceTimeSeries(rawData) {
        const startTime = Date.now();
        console.log(`  📊 시계열 데이터 처리 시작: ${rawData ? rawData.length : 0}개 레코드`);

        if (!Array.isArray(rawData) || rawData.length === 0) {
            console.log(`  ⚠️ 시계열 데이터 처리 건너뜀: 빈 데이터`);
            return {
                labels: [],
                datasets: []
            };
        }

        // 자료생성년월별로 데이터 그룹화 및 정렬
        const groupStartTime = Date.now();
        const groupedData = this.groupByMonth(rawData);
        const groupTime = ((Date.now() - groupStartTime) / 1000).toFixed(3);
        console.log(`    ⚙️ 데이터 그룹화 완료 (${groupTime}초)`);
        const sortedMonths = Object.keys(groupedData).sort();

        // 레이블 생성 (YYYY-MM 형식)
        const labels = sortedMonths.map(month =>
            moment(month, 'YYYYMM').format('YYYY-MM')
        );

        // 각 월별 데이터 집계
        const newHires = [];
        const resignations = [];
        const totalMembers = [];
        const estimatedSalaries = [];

        sortedMonths.forEach(month => {
            const monthData = groupedData[month];

            // 해당 월의 모든 데이터를 합산
            const totals = monthData.reduce((acc, item) => {
                acc.newAcqs += this.parseNumber(item['신규취득자수']);
                acc.loss += this.parseNumber(item['상실가입자수']);
                acc.total += this.parseNumber(item['가입자수']);
                acc.totalAmount += this.parseNumber(item['당월고지금액']);
                return acc;
            }, { newAcqs: 0, loss: 0, total: 0, totalAmount: 0 });

            newHires.push(totals.newAcqs);
            resignations.push(totals.loss);
            totalMembers.push(totals.total);

            // 급여 추정 계산 (연도별 보험료율 적용)
            const 월국민연금금액 = totals.total > 0 ? Math.round(totals.totalAmount / totals.total) : 0;
            const incomeEstimation = estimateMonthlyIncome(월국민연금금액, month);
            const 월급여추정 = Math.round(incomeEstimation.estimatedIncome / 10000); // 만원 단위
            estimatedSalaries.push(월급여추정);
        });

        const endTime = Date.now();
        const totalTime = ((endTime - startTime) / 1000).toFixed(3);
        console.log(`  ✅ 시계열 데이터 처리 완료 (${totalTime}초)`);

        return {
            labels,
            datasets: [
                {
                    label: '신규입사자',
                    data: newHires,
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    tension: 0.1
                },
                {
                    label: '퇴사자',
                    data: resignations,
                    borderColor: 'rgb(255, 99, 132)',
                    backgroundColor: 'rgba(255, 99, 132, 0.2)',
                    tension: 0.1
                },
                {
                    label: '총 인원',
                    data: totalMembers,
                    borderColor: 'rgb(54, 162, 235)',
                    backgroundColor: 'rgba(54, 162, 235, 0.2)',
                    tension: 0.1,
                    yAxisID: 'y1'
                },
                {
                    label: '월급여추정 (만원)',
                    data: estimatedSalaries,
                    borderColor: 'rgb(255, 206, 86)',
                    backgroundColor: 'rgba(255, 206, 86, 0.2)',
                    tension: 0.1,
                    yAxisID: 'y2'
                }
            ]
        };
    }

    processWorkplaceSummary(rawData) {
        const startTime = Date.now();
        console.log(`  📊 요약 데이터 처리 시작: ${rawData ? rawData.length : 0}개 레코드`);

        if (!Array.isArray(rawData) || rawData.length === 0) {
            console.log(`  ⚠️ 요약 데이터 처리 건너뜀: 빈 데이터`);
            return {
                totalNewHires: 0,
                totalResignations: 0,
                currentTotal: 0,
                averageMonthlyChange: 0,
                monthlyData: []
            };
        }

        const groupedData = this.groupByMonth(rawData);
        const sortedMonths = Object.keys(groupedData).sort();

        let totalNewHires = 0;
        let totalResignations = 0;
        let currentTotal = 0;
        const monthlyChanges = [];
        const monthlyData = [];

        sortedMonths.forEach(month => {
            const monthData = groupedData[month];

            const totals = monthData.reduce((acc, item) => {
                acc.newAcqs += this.parseNumber(item['신규취득자수']);
                acc.loss += this.parseNumber(item['상실가입자수']);
                acc.total += this.parseNumber(item['가입자수']);
                acc.totalAmount += this.parseNumber(item['당월고지금액']);
                return acc;
            }, { newAcqs: 0, loss: 0, total: 0, totalAmount: 0 });

            totalNewHires += totals.newAcqs;
            totalResignations += totals.loss;
            currentTotal = totals.total; // 가장 최근 월의 총 인원

            const netChange = totals.newAcqs - totals.loss;
            monthlyChanges.push(netChange);

            // 해당 월의 첫 번째 레코드에서 사업장 정보 가져오기
            const firstRecord = monthData[0] || {};
            const workplaceName = firstRecord['사업장명'] || '';
            const businessRegNo = firstRecord['사업자등록번호'] || '';

            // 급여 추정 계산 (연도별 보험료율 및 상한액 적용)
            const 월국민연금금액 = totals.total > 0 ? Math.round(totals.totalAmount / totals.total) : 0;
            const 개인납부국민연금금액 = Math.round(월국민연금금액 / 2);
            
            // 해당 월의 연금 기준 정보 조회
            const incomeEstimation = estimateMonthlyIncome(월국민연금금액, month);
            const 월급여추정 = Math.round(incomeEstimation.estimatedIncome / 10000); // 만원 단위
            const 연간급여추정 = 월급여추정 * 12;
            
            // 상한액/하한액 도달 여부
            const 상한액도달 = incomeEstimation.isAtUpperLimit;
            const 하한액도달 = incomeEstimation.isAtLowerLimit;
            const 기준소득월액상한액 = incomeEstimation.limits.upper;
            const 기준소득월액하한액 = incomeEstimation.limits.lower;
            const 적용보험료율 = incomeEstimation.limits.rate;

            monthlyData.push({
                month: moment(month, 'YYYYMM').format('YYYY-MM'),
                사업장명: workplaceName,
                사업자등록번호: businessRegNo,
                newHires: totals.newAcqs,
                resignations: totals.loss,
                total: totals.total,
                netChange,
                당월고지금액: totals.totalAmount,
                월국민연금금액,
                개인납부국민연금금액,
                월급여추정,
                연간급여추정,
                상한액도달,
                하한액도달,
                기준소득월액상한액,
                기준소득월액하한액,
                적용보험료율
            });
        });

        const averageMonthlyChange = monthlyChanges.length > 0
            ? monthlyChanges.reduce((a, b) => a + b, 0) / monthlyChanges.length
            : 0;

        const endTime = Date.now();
        const totalTime = ((endTime - startTime) / 1000).toFixed(3);
        console.log(`  ✅ 요약 데이터 처리 완료 (${totalTime}초)`);

        return {
            totalNewHires,
            totalResignations,
            currentTotal,
            averageMonthlyChange: Math.round(averageMonthlyChange * 100) / 100,
            monthlyData
        };
    }

    processWorkplaceComparison(workplacesData) {
        const comparison = [];

        Object.keys(workplacesData).forEach(workplaceName => {
            const data = workplacesData[workplaceName];
            const summary = this.processWorkplaceSummary(data);

            comparison.push({
                name: workplaceName,
                ...summary
            });
        });

        // 현재 총 인원수로 정렬
        return comparison.sort((a, b) => b.currentTotal - a.currentTotal);
    }

    groupByMonth(data) {
        return data.reduce((acc, item) => {
            const month = item['자료생성년월'];
            if (!acc[month]) {
                acc[month] = [];
            }
            acc[month].push(item);
            return acc;
        }, {});
    }

    parseNumber(value) {
        if (value === null || value === undefined || value === '') {
            return 0;
        }

        // 문자열로 저장된 숫자도 처리
        const parsed = parseInt(String(value), 10);
        return isNaN(parsed) ? 0 : parsed;
    }

    filterDataByDateRange(data, startDate, endDate) {
        const start = moment(startDate, 'YYYY-MM');
        const end = moment(endDate, 'YYYY-MM');

        return data.filter(item => {
            const dateValue = item['자료생성년월'];
            if (!dateValue) return false;

            // 데이터 형식 확인 및 변환
            let itemDate;
            if (dateValue.length === 6 && /^\d{6}$/.test(dateValue)) {
                // YYYYMM 형식인 경우 (예: 201605)
                itemDate = moment(dateValue, 'YYYYMM');
            } else if (dateValue.length === 7 && /^\d{4}-\d{2}$/.test(dateValue)) {
                // YYYY-MM 형식인 경우 (예: 2016-05)
                itemDate = moment(dateValue, 'YYYY-MM');
            } else {
                // 기타 형식 시도
                itemDate = moment(dateValue);
            }

            if (!itemDate.isValid()) return false;

            return itemDate.isSameOrAfter(start) && itemDate.isSameOrBefore(end);
        });
    }

    getWorkplaceList(data) {
        const workplaces = new Set();
        data.forEach(item => {
            if (item['사업장명'] && item['사업장명'].trim()) {
                workplaces.add(item['사업장명'].trim());
            }
        });
        return Array.from(workplaces).sort();
    }

    generateStatistics(data) {
        const startTime = Date.now();
        console.log(`  📊 통계 데이터 처리 시작: ${data ? data.length : 0}개 레코드`);

        if (!Array.isArray(data) || data.length === 0) {
            console.log(`  ⚠️ 통계 데이터 처리 건너뜀: 빈 데이터`);
            return {
                dataPoints: 0,
                dateRange: { start: null, end: null },
                workplaceCount: 0
            };
        }

        const months = data.map(item => item['자료생성년월']).filter(Boolean);
        const sortedMonths = months.sort();
        const workplaces = this.getWorkplaceList(data);

        const endTime = Date.now();
        const totalTime = ((endTime - startTime) / 1000).toFixed(3);
        console.log(`  ✅ 통계 데이터 처리 완료 (${totalTime}초)`);

        return {
            dataPoints: data.length,
            dateRange: {
                start: sortedMonths.length > 0 ? moment(sortedMonths[0], 'YYYY-MM').format('YYYY-MM') : null,
                end: sortedMonths.length > 0 ? moment(sortedMonths[sortedMonths.length - 1], 'YYYY-MM').format('YYYY-MM') : null
            },
            workplaceCount: workplaces.length
        };
    }
}

module.exports = DataProcessor;