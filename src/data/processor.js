const moment = require('moment');

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
        if (!Array.isArray(rawData) || rawData.length === 0) {
            return {
                labels: [],
                datasets: []
            };
        }

        // 자료생성년월별로 데이터 그룹화 및 정렬
        const groupedData = this.groupByMonth(rawData);
        const sortedMonths = Object.keys(groupedData).sort();

        // 레이블 생성 (YYYY-MM 형식)
        const labels = sortedMonths.map(month =>
            moment(month, 'YYYYMM').format('YYYY-MM')
        );

        // 각 월별 데이터 집계
        const newHires = [];
        const resignations = [];
        const totalMembers = [];

        sortedMonths.forEach(month => {
            const monthData = groupedData[month];

            // 해당 월의 모든 데이터를 합산
            const totals = monthData.reduce((acc, item) => {
                acc.newAcqs += this.parseNumber(item['신규취득자수']);
                acc.loss += this.parseNumber(item['상실가입자수']);
                acc.total += this.parseNumber(item['가입자수']);
                return acc;
            }, { newAcqs: 0, loss: 0, total: 0 });

            newHires.push(totals.newAcqs);
            resignations.push(totals.loss);
            totalMembers.push(totals.total);
        });

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
                }
            ]
        };
    }

    processWorkplaceSummary(rawData) {
        if (!Array.isArray(rawData) || rawData.length === 0) {
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
                return acc;
            }, { newAcqs: 0, loss: 0, total: 0 });

            totalNewHires += totals.newAcqs;
            totalResignations += totals.loss;
            currentTotal = totals.total; // 가장 최근 월의 총 인원

            const netChange = totals.newAcqs - totals.loss;
            monthlyChanges.push(netChange);

            monthlyData.push({
                month: moment(month, 'YYYYMM').format('YYYY-MM'),
                newHires: totals.newAcqs,
                resignations: totals.loss,
                total: totals.total,
                netChange
            });
        });

        const averageMonthlyChange = monthlyChanges.length > 0
            ? monthlyChanges.reduce((a, b) => a + b, 0) / monthlyChanges.length
            : 0;

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

            return itemDate.isBetween(start, end, null, '[]');
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
        if (!Array.isArray(data) || data.length === 0) {
            return {
                dataPoints: 0,
                dateRange: { start: null, end: null },
                workplaceCount: 0
            };
        }

        const months = data.map(item => item['자료생성년월']).filter(Boolean);
        const sortedMonths = months.sort();
        const workplaces = this.getWorkplaceList(data);

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