class PensionVisualization {
    constructor() {
        this.charts = {};
        this.currentData = null;
        this.init();
    }

    init() {
        this.bindEvents();
        this.setDefaultDates();
    }

    bindEvents() {
        document.getElementById('searchBtn').addEventListener('click', () => {
            this.searchWorkplaceData();
        });

        document.getElementById('compareBtn').addEventListener('click', () => {
            this.compareWorkplaces();
        });

        // Enter 키로 검색
        document.getElementById('workplaceName').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.searchWorkplaceData();
            }
        });
    }

    setDefaultDates() {
        const endDate = new Date();
        const startDate = new Date();
        startDate.setFullYear(endDate.getFullYear() - 1);

        document.getElementById('startDate').value = this.formatDateForInput(startDate);
        document.getElementById('endDate').value = this.formatDateForInput(endDate);
    }

    formatDateForInput(date) {
        const year = date.getFullYear();
        const month = (date.getMonth() + 1).toString().padStart(2, '0');
        return `${year}-${month}`;
    }

    async searchWorkplaceData() {
        const workplaceName = document.getElementById('workplaceName').value.trim();
        const startDate = document.getElementById('startDate').value;
        const endDate = document.getElementById('endDate').value;

        if (!workplaceName) {
            this.showError('사업장명을 입력해주세요.');
            return;
        }

        if (!startDate || !endDate) {
            this.showError('시작 기간과 종료 기간을 모두 입력해주세요.');
            return;
        }

        if (new Date(startDate) > new Date(endDate)) {
            this.showError('시작 기간이 종료 기간보다 늦을 수 없습니다.');
            return;
        }

        this.showLoading();

        try {
            const response = await fetch('/api/workplace-data', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    workplaceName,
                    startDate,
                    endDate
                })
            });

            const result = await response.json();

            if (result.success) {
                this.currentData = result.data;
                this.displayData(result.data, workplaceName);
            } else {
                this.showError(result.error || '데이터를 불러오는데 실패했습니다.');
            }
        } catch (error) {
            console.error('API 호출 오류:', error);
            this.showError('서버 연결에 실패했습니다.');
        } finally {
            this.hideLoading();
        }
    }

    async compareWorkplaces() {
        const workplaceNames = document.getElementById('workplaceName').value
            .split(',')
            .map(name => name.trim())
            .filter(name => name.length > 0);

        if (workplaceNames.length < 2) {
            this.showError('비교할 사업장명을 쉼표로 구분하여 2개 이상 입력해주세요. (예: 삼성전자, LG전자)');
            return;
        }

        const startDate = document.getElementById('startDate').value;
        const endDate = document.getElementById('endDate').value;

        this.showLoading();

        try {
            const response = await fetch('/api/compare-workplaces', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    workplaceNames,
                    startDate,
                    endDate
                })
            });

            const result = await response.json();

            if (result.success) {
                this.displayComparisonData(result.data, workplaceNames);
            } else {
                this.showError(result.error || '비교 데이터를 불러오는데 실패했습니다.');
            }
        } catch (error) {
            console.error('비교 API 호출 오류:', error);
            this.showError('서버 연결에 실패했습니다.');
        } finally {
            this.hideLoading();
        }
    }

    displayData(data, workplaceName) {
        this.updateSummary(data.summary);
        this.createTimeSeriesChart(data.chartData, workplaceName);
        this.createMonthlyChart(data.chartData, workplaceName);
        this.updateTable(data.summary.monthlyData);
        this.showDataInfo();
    }

    displayComparisonData(data, workplaceNames) {
        this.createComparisonChart(data);
        this.updateComparisonTable(data);
        this.showDataInfo();
        this.updateSummaryForComparison(data);
    }

    updateSummary(summary) {
        document.getElementById('totalNewHires').textContent = summary.totalNewHires.toLocaleString() + '명';
        document.getElementById('totalResignations').textContent = summary.totalResignations.toLocaleString() + '명';
        document.getElementById('currentTotal').textContent = summary.currentTotal.toLocaleString() + '명';

        const changeValue = summary.averageMonthlyChange;
        const changeText = changeValue >= 0 ? `+${changeValue}명` : `${changeValue}명`;
        const changeColor = changeValue >= 0 ? '#28a745' : '#dc3545';

        const changeElement = document.getElementById('averageChange');
        changeElement.textContent = changeText;
        changeElement.style.color = changeColor;
    }

    updateSummaryForComparison(comparisonData) {
        const totalNewHires = comparisonData.reduce((sum, item) => sum + item.totalNewHires, 0);
        const totalResignations = comparisonData.reduce((sum, item) => sum + item.totalResignations, 0);
        const totalCurrent = comparisonData.reduce((sum, item) => sum + item.currentTotal, 0);
        const avgChange = comparisonData.reduce((sum, item) => sum + item.averageMonthlyChange, 0) / comparisonData.length;

        document.getElementById('totalNewHires').textContent = totalNewHires.toLocaleString() + '명';
        document.getElementById('totalResignations').textContent = totalResignations.toLocaleString() + '명';
        document.getElementById('currentTotal').textContent = totalCurrent.toLocaleString() + '명';

        const changeText = avgChange >= 0 ? `+${avgChange.toFixed(1)}명` : `${avgChange.toFixed(1)}명`;
        const changeColor = avgChange >= 0 ? '#28a745' : '#dc3545';

        const changeElement = document.getElementById('averageChange');
        changeElement.textContent = changeText;
        changeElement.style.color = changeColor;
    }

    createTimeSeriesChart(chartData, workplaceName) {
        const ctx = document.getElementById('timeSeriesChart').getContext('2d');

        if (this.charts.timeSeries) {
            this.charts.timeSeries.destroy();
        }

        this.charts.timeSeries = new Chart(ctx, {
            type: 'line',
            data: chartData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: `${workplaceName} - 시간별 인원 변화`,
                        font: {
                            size: 16
                        }
                    },
                    legend: {
                        position: 'top',
                    }
                },
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                scales: {
                    x: {
                        display: true,
                        title: {
                            display: true,
                            text: '기간'
                        }
                    },
                    y: {
                        type: 'linear',
                        display: true,
                        position: 'left',
                        title: {
                            display: true,
                            text: '입사자/퇴사자 (명)'
                        }
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        title: {
                            display: true,
                            text: '총 인원 (명)'
                        },
                        grid: {
                            drawOnChartArea: false,
                        },
                    }
                }
            }
        });
    }

    createMonthlyChart(chartData, workplaceName) {
        const ctx = document.getElementById('monthlyChart').getContext('2d');

        if (this.charts.monthly) {
            this.charts.monthly.destroy();
        }

        // 순 변화 데이터 생성
        const netChangeData = chartData.datasets[0].data.map((hire, index) =>
            hire - chartData.datasets[1].data[index]
        );

        const monthlyChartData = {
            labels: chartData.labels,
            datasets: [
                {
                    label: '신규입사자',
                    data: chartData.datasets[0].data,
                    backgroundColor: 'rgba(75, 192, 192, 0.6)',
                    borderColor: 'rgba(75, 192, 192, 1)',
                    borderWidth: 1
                },
                {
                    label: '퇴사자',
                    data: chartData.datasets[1].data,
                    backgroundColor: 'rgba(255, 99, 132, 0.6)',
                    borderColor: 'rgba(255, 99, 132, 1)',
                    borderWidth: 1
                },
                {
                    label: '순 변화',
                    data: netChangeData,
                    backgroundColor: 'rgba(255, 206, 86, 0.6)',
                    borderColor: 'rgba(255, 206, 86, 1)',
                    borderWidth: 1
                }
            ]
        };

        this.charts.monthly = new Chart(ctx, {
            type: 'bar',
            data: monthlyChartData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: `${workplaceName} - 월별 인원 변화`,
                        font: {
                            size: 16
                        }
                    },
                    legend: {
                        position: 'top',
                    }
                },
                scales: {
                    x: {
                        display: true,
                        title: {
                            display: true,
                            text: '기간'
                        }
                    },
                    y: {
                        display: true,
                        title: {
                            display: true,
                            text: '인원수 (명)'
                        }
                    }
                }
            }
        });
    }

    createComparisonChart(comparisonData) {
        const ctx = document.getElementById('timeSeriesChart').getContext('2d');

        if (this.charts.timeSeries) {
            this.charts.timeSeries.destroy();
        }

        const labels = comparisonData.map(item => item.name);
        const datasets = [
            {
                label: '현재 총 인원',
                data: comparisonData.map(item => item.currentTotal),
                backgroundColor: 'rgba(54, 162, 235, 0.6)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 1
            },
            {
                label: '총 신규입사자',
                data: comparisonData.map(item => item.totalNewHires),
                backgroundColor: 'rgba(75, 192, 192, 0.6)',
                borderColor: 'rgba(75, 192, 192, 1)',
                borderWidth: 1
            },
            {
                label: '총 퇴사자',
                data: comparisonData.map(item => item.totalResignations),
                backgroundColor: 'rgba(255, 99, 132, 0.6)',
                borderColor: 'rgba(255, 99, 132, 1)',
                borderWidth: 1
            }
        ];

        this.charts.timeSeries = new Chart(ctx, {
            type: 'bar',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: '사업장별 인원 비교',
                        font: {
                            size: 16
                        }
                    },
                    legend: {
                        position: 'top',
                    }
                },
                scales: {
                    x: {
                        display: true,
                        title: {
                            display: true,
                            text: '사업장'
                        }
                    },
                    y: {
                        display: true,
                        title: {
                            display: true,
                            text: '인원수 (명)'
                        }
                    }
                }
            }
        });

        // 월별 차트는 숨김
        if (this.charts.monthly) {
            this.charts.monthly.destroy();
        }
        document.getElementById('monthlyChart').style.display = 'none';
    }

    updateTable(monthlyData) {
        const tbody = document.getElementById('dataTableBody');
        tbody.innerHTML = '';

        if (!monthlyData || monthlyData.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="no-data">데이터가 없습니다</td></tr>';
            return;
        }

        monthlyData.forEach(item => {
            const row = document.createElement('tr');

            const netChangeClass = item.netChange >= 0 ? 'text-success' : 'text-danger';
            const netChangeSymbol = item.netChange >= 0 ? '+' : '';

            row.innerHTML = `
                <td>${item.month}</td>
                <td>${item.newHires.toLocaleString()}명</td>
                <td>${item.resignations.toLocaleString()}명</td>
                <td>${item.total.toLocaleString()}명</td>
                <td class="${netChangeClass}">${netChangeSymbol}${item.netChange.toLocaleString()}명</td>
            `;

            tbody.appendChild(row);
        });
    }

    updateComparisonTable(comparisonData) {
        const tbody = document.getElementById('dataTableBody');
        tbody.innerHTML = '';

        // 테이블 헤더 변경
        const thead = document.querySelector('#dataTable thead tr');
        thead.innerHTML = `
            <th>사업장명</th>
            <th>총 신규입사자</th>
            <th>총 퇴사자</th>
            <th>현재 총 인원</th>
            <th>월평균 변화</th>
        `;

        comparisonData.forEach(item => {
            const row = document.createElement('tr');

            const avgChangeClass = item.averageMonthlyChange >= 0 ? 'text-success' : 'text-danger';
            const avgChangeSymbol = item.averageMonthlyChange >= 0 ? '+' : '';

            row.innerHTML = `
                <td>${item.name}</td>
                <td>${item.totalNewHires.toLocaleString()}명</td>
                <td>${item.totalResignations.toLocaleString()}명</td>
                <td>${item.currentTotal.toLocaleString()}명</td>
                <td class="${avgChangeClass}">${avgChangeSymbol}${item.averageMonthlyChange.toFixed(1)}명</td>
            `;

            tbody.appendChild(row);
        });
    }

    showLoading() {
        document.getElementById('loadingIndicator').classList.remove('hidden');
        this.hideError();
        this.hideDataInfo();
    }

    hideLoading() {
        document.getElementById('loadingIndicator').classList.add('hidden');
    }

    showError(message) {
        const errorElement = document.getElementById('errorMessage');
        errorElement.textContent = message;
        errorElement.classList.remove('hidden');
        this.hideDataInfo();
    }

    hideError() {
        document.getElementById('errorMessage').classList.add('hidden');
    }

    showDataInfo() {
        document.getElementById('dataInfo').classList.remove('hidden');
        this.hideError();
    }

    hideDataInfo() {
        document.getElementById('dataInfo').classList.add('hidden');
    }
}

// CSS 스타일 추가
const style = document.createElement('style');
style.textContent = `
    .text-success {
        color: #28a745 !important;
    }

    .text-danger {
        color: #dc3545 !important;
    }
`;
document.head.appendChild(style);

// 페이지 로드 시 초기화
document.addEventListener('DOMContentLoaded', () => {
    new PensionVisualization();
});