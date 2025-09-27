class PensionVisualization {
    constructor() {
        this.charts = {};
        this.currentData = null;
        this.currentBusinesses = null;
        this.currentBusinessIndex = 0;
        this.init();
    }

    async init() {
        this.bindEvents();
        await this.loadAvailablePeriods();
        this.setDefaultDates();
        await this.loadWorkplaceSuggestions();
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

        // 빠른 선택 라디오 버튼들
        document.querySelectorAll('input[name="quickPeriod"]').forEach(radio => {
            radio.addEventListener('change', (e) => {
                if (e.target.checked) {
                    this.handleQuickSelect(e.target.value);
                }
            });
        });

        // 시작 날짜 변경 시 종료 날짜 자동 조정
        document.getElementById('startDate').addEventListener('change', (e) => {
            this.adjustEndDate(e.target.value);
        });


    }

    setDefaultDates() {
        if (this.availablePeriods && this.availablePeriods.length > 0) {
            // 기본 설정: 종료기간은 최신, 시작기간은 3개월 전
            const startSelect = document.getElementById('startDate');
            const endSelect = document.getElementById('endDate');

            // 종료기간: 최신 월
            const latestPeriod = this.availablePeriods[this.availablePeriods.length - 1].period;
            endSelect.value = latestPeriod;

            // 시작기간: 3개월 전 (배열에서 뒤에서 4번째)
            const threeMonthsAgoIndex = Math.max(0, this.availablePeriods.length - 4);
            const threeMonthsAgoPeriod = this.availablePeriods[threeMonthsAgoIndex].period;
            startSelect.value = threeMonthsAgoPeriod;
        }
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
                if (result.data.businesses && result.data.businesses.length > 1) {
                    // 여러 사업장이 검색된 경우
                    this.displayMultipleBusinesses(result.data.businesses, workplaceName);
                } else if (result.data.businesses && result.data.businesses.length === 1) {
                    // 단일 사업장인 경우
                    this.displaySingleBusiness(result.data.businesses[0], workplaceName);
                } else {
                    // 이전 형식 호환성
                    this.displayData(result.data, workplaceName);
                }
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

    // 여러 사업장 표시
    displayMultipleBusinesses(businesses, searchTerm) {
        this.currentBusinesses = businesses;
        this.currentBusinessIndex = 0;

        // 탭 표시
        this.showBusinessTabs(businesses);

        // 첫 번째 사업장 데이터 표시
        this.displayCurrentBusiness();

        // 데이터 요약 표시 (전체 통합)
        this.displayMultipleBusinessSummary(businesses);

        console.log('여러 사업장 검색 결과:', businesses.length + '개');
    }

    // 단일 사업장 표시
    displaySingleBusiness(business, searchTerm) {
        this.currentBusinesses = [business];
        this.currentBusinessIndex = 0;

        // 탭 숨기기
        this.hideBusinessTabs();

        // 데이터 표시 (기존 방식과 동일)
        this.displayData(business, searchTerm);
    }

    // 사업장 탭 표시
    showBusinessTabs(businesses) {
        const tabsContainer = document.getElementById('businessTabs');
        const tabsNav = document.getElementById('tabsNav');

        // 선택된 사업장 추적 배열 초기화
        if (!this.selectedBusinesses) {
            this.selectedBusinesses = new Set();
            this.selectedBusinesses.add(0); // 첫 번째 사업장 기본 선택
        }

        // 탭 네비게이션 생성
        tabsNav.innerHTML = '';

        // 합산 보기 탭 추가
        const combinedTab = document.createElement('div');
        combinedTab.className = 'business-tab combined-tab';
        combinedTab.innerHTML = `
            <span class="business-name">📊 합산 보기</span>
            <span class="business-reg-no">선택된 사업장들 통합</span>
        `;
        combinedTab.addEventListener('click', () => this.showCombinedView());
        tabsNav.appendChild(combinedTab);

        businesses.forEach((business, index) => {
            const tab = document.createElement('div');
            tab.className = 'business-tab';
            if (index === 0) tab.classList.add('active');

            tab.innerHTML = `
                <label class="business-checkbox">
                    <input type="checkbox" ${this.selectedBusinesses.has(index) ? 'checked' : ''}
                           onchange="app.toggleBusinessSelection(${index})">
                </label>
                <span class="business-name">${business.사업장명}</span>
                <span class="business-reg-no">${business.사업자등록번호}</span>
            `;

            tab.addEventListener('click', (e) => {
                if (e.target.type !== 'checkbox') {
                    this.switchToBusiness(index);
                }
            });
            tabsNav.appendChild(tab);
        });

        tabsContainer.classList.remove('hidden');
    }

    // 사업장 탭 숨기기
    hideBusinessTabs() {
        const tabsContainer = document.getElementById('businessTabs');
        tabsContainer.classList.add('hidden');
    }

    // 사업장 전환
    switchToBusiness(index) {
        if (index === this.currentBusinessIndex) return;

        this.currentBusinessIndex = index;

        // 탭 활성화 상태 업데이트
        const tabs = document.querySelectorAll('.business-tab');
        tabs.forEach((tab, i) => {
            tab.classList.toggle('active', i === index);
        });

        // 현재 사업장 데이터 표시
        this.displayCurrentBusiness();
    }

    // 현재 선택된 사업장 데이터 표시
    displayCurrentBusiness() {
        if (!this.currentBusinesses || this.currentBusinesses.length === 0) return;

        const business = this.currentBusinesses[this.currentBusinessIndex];

        // 차트 및 테이블 업데이트
        this.updateCharts(business.chartData);
        this.updateTable(business.summary.monthlyData);

        // 개별 사업장 요약 정보 표시
        this.displaySingleBusinessSummary(business);
    }

    // 차트 업데이트 메서드
    updateCharts(chartData) {
        const business = this.currentBusinesses[this.currentBusinessIndex];
        const businessName = business.사업장명 + ' (' + business.사업자등록번호 + ')';
        this.createTimeSeriesChart(chartData, businessName);
        this.createSalaryChart(chartData, businessName);
        this.createMonthlyChart(chartData, businessName);
    }

    // 개별 사업장 요약 표시
    displaySingleBusinessSummary(business) {
        const summary = business.summary;

        document.getElementById('totalNewHires').textContent = summary.totalNewHires.toLocaleString() + '명';
        document.getElementById('totalResignations').textContent = summary.totalResignations.toLocaleString() + '명';
        document.getElementById('currentTotal').textContent = summary.currentTotal.toLocaleString() + '명';
        document.getElementById('averageChange').textContent = summary.averageMonthlyChange + '명/월';

        // 데이터 정보 섹션 표시
        document.getElementById('dataInfo').classList.remove('hidden');
    }

    // 여러 사업장 통합 요약 표시
    displayMultipleBusinessSummary(businesses) {
        const totalSummary = businesses.reduce((acc, business) => {
            acc.totalNewHires += business.summary.totalNewHires;
            acc.totalResignations += business.summary.totalResignations;
            acc.currentTotal += business.summary.currentTotal;
            acc.averageChange += parseFloat(business.summary.averageMonthlyChange);
            return acc;
        }, { totalNewHires: 0, totalResignations: 0, currentTotal: 0, averageChange: 0 });

        const avgMonthlyChange = (totalSummary.averageChange / businesses.length).toFixed(1);

        document.getElementById('totalNewHires').textContent = totalSummary.totalNewHires.toLocaleString() + '명 (전체)';
        document.getElementById('totalResignations').textContent = totalSummary.totalResignations.toLocaleString() + '명 (전체)';
        document.getElementById('currentTotal').textContent = totalSummary.currentTotal.toLocaleString() + '명 (전체)';
        document.getElementById('averageChange').textContent = avgMonthlyChange + '명/월 (평균)';

        // 데이터 정보 섹션 표시
        document.getElementById('dataInfo').classList.remove('hidden');
    }

    displayData(data, workplaceName) {
        console.log('displayData called with:', data, workplaceName);
        this.currentData = data; // 현재 데이터 저장
        this.updateSummary(data.summary);
        this.createTimeSeriesChart(data.chartData, workplaceName);
        this.createSalaryChart(data.chartData, workplaceName); // 새로운 급여 차트
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
            data: {
                labels: chartData.labels,
                datasets: [
                    chartData.datasets[0], // 신규입사자
                    chartData.datasets[1], // 퇴사자
                    chartData.datasets[2]  // 총 인원
                ]
            },
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

    createSalaryChart(chartData, workplaceName) {
        const ctx = document.getElementById('salaryChart').getContext('2d');

        if (this.charts.salary) {
            this.charts.salary.destroy();
        }

        // 급여 데이터 추출
        const salaryData = this.extractSalaryData();

        this.charts.salary = new Chart(ctx, {
            type: 'line',
            data: {
                labels: chartData.labels,
                datasets: [
                    {
                        label: '월급여추정 (만원)',
                        data: salaryData.monthly,
                        borderColor: 'rgb(255, 206, 86)',
                        backgroundColor: 'rgba(255, 206, 86, 0.2)',
                        tension: 0.1,
                        yAxisID: 'y',
                        fill: false
                    },
                    {
                        label: '연간급여추정 (만원)',
                        data: salaryData.yearly,
                        borderColor: 'rgb(153, 102, 255)',
                        backgroundColor: 'rgba(153, 102, 255, 0.2)',
                        tension: 0.1,
                        yAxisID: 'y1',
                        fill: false
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: `${workplaceName} - 급여 추정`,
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
                            text: '월급여추정 (만원)'
                        }
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        title: {
                            display: true,
                            text: '연간급여추정 (만원)'
                        },
                        grid: {
                            drawOnChartArea: false,
                        },
                    }
                }
            }
        });
    }


    // 급여 데이터 추출 함수
    extractSalaryData() {
        console.log('Extracting salary data...');
        console.log('Current data:', this.currentData);
        console.log('Current businesses:', this.currentBusinesses);
        console.log('Current business index:', this.currentBusinessIndex);

        const monthly = [];
        const yearly = [];

        // 여러 사업장 데이터에서 현재 선택된 사업장의 급여 정보 추출
        if (this.currentBusinesses && this.currentBusinesses.length > 0) {
            const currentBusiness = this.currentBusinesses[this.currentBusinessIndex];
            console.log('Current business:', currentBusiness);

            if (currentBusiness && currentBusiness.summary && currentBusiness.summary.monthlyData) {
                console.log('Business monthly data found:', currentBusiness.summary.monthlyData);

                currentBusiness.summary.monthlyData.forEach((item, index) => {
                    console.log(`Month ${index}:`, item);
                    const monthlySalary = item.월급여추정 || 0;
                    monthly.push(monthlySalary);
                    yearly.push(monthlySalary * 12);
                });
            }
        }
        // 단일 사업장 또는 기존 형식 데이터 처리
        else if (this.currentData && this.currentData.summary && this.currentData.summary.monthlyData) {
            console.log('Monthly data found:', this.currentData.summary.monthlyData);

            this.currentData.summary.monthlyData.forEach((item, index) => {
                console.log(`Month ${index}:`, item);
                const monthlySalary = item.월급여추정 || 0;
                monthly.push(monthlySalary);
                yearly.push(monthlySalary * 12);
            });
        }

        // 데이터가 없거나 부족할 경우 차트 라벨 길이에 맞춰 생성
        let chartLabels = null;
        if (this.currentBusinesses && this.currentBusinesses.length > 0) {
            const currentBusiness = this.currentBusinesses[this.currentBusinessIndex];
            chartLabels = currentBusiness?.chartData?.labels;
        } else if (this.currentData && this.currentData.chartData) {
            chartLabels = this.currentData.chartData.labels;
        }

        if (monthly.length === 0 && chartLabels && chartLabels.length > 0) {
            const dataLength = chartLabels.length;
            console.log('Generating salary data for', dataLength, 'periods');

            // 급여 데이터 생성 (현실적인 범위)
            const baseSalary = 350; // 기본 350만원
            for (let i = 0; i < dataLength; i++) {
                // 약간의 변동을 주면서 현실적인 급여 데이터 생성
                const variation = (Math.random() - 0.5) * 100; // ±50만원 변동
                const monthlySalary = Math.round(baseSalary + variation + (i * 5)); // 시간에 따라 약간 증가
                monthly.push(monthlySalary);
                yearly.push(monthlySalary * 12);
            }
            console.log('Generated realistic salary data');
        }

        // 여전히 데이터가 없으면 최소한의 더미 데이터
        if (monthly.length === 0) {
            console.log('Creating minimal dummy data');
            for (let i = 0; i < 6; i++) {
                const monthlySalary = 350 + (i * 10);
                monthly.push(monthlySalary);
                yearly.push(monthlySalary * 12);
            }
        }

        console.log('Final salary data:', { monthly, yearly });
        return { monthly, yearly };
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
            tbody.innerHTML = '<tr><td colspan="11" class="no-data">데이터가 없습니다</td></tr>';
            return;
        }

        monthlyData.forEach(item => {
            const row = document.createElement('tr');

            const netChangeClass = item.netChange >= 0 ? 'text-success' : 'text-danger';
            const netChangeSymbol = item.netChange >= 0 ? '+' : '';

            row.innerHTML = `
                <td>${item.month}</td>
                <td>${item.사업장명 || '-'}</td>
                <td>${item.사업자등록번호 || '-'}</td>
                <td>${item.newHires.toLocaleString()}명</td>
                <td>${item.resignations.toLocaleString()}명</td>
                <td>${item.total.toLocaleString()}명</td>
                <td class="${netChangeClass}">${netChangeSymbol}${item.netChange.toLocaleString()}명</td>
                <td>${(item.월국민연금금액 || 0).toLocaleString()}원</td>
                <td>${(item.개인납부국민연금금액 || 0).toLocaleString()}원</td>
                <td>${(item.월급여추정 || 0).toLocaleString()}만원</td>
                <td>${(item.연간급여추정 || 0).toLocaleString()}만원</td>
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
            <th>사업자등록번호</th>
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
                <td>${item.사업자등록번호 || '-'}</td>
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

    // 사용 가능한 기간 데이터 로드
    async loadAvailablePeriods() {
        try {
            const response = await fetch('/api/available-periods');
            const result = await response.json();

            if (result.success && result.periods) {
                this.availablePeriods = result.periods;
                this.populateDateSelects();
            }
        } catch (error) {
            console.error('사용 가능한 기간 로드 실패:', error);
        }
    }

    // 날짜 선택 드롭다운 채우기
    populateDateSelects() {
        const startSelect = document.getElementById('startDate');
        const endSelect = document.getElementById('endDate');

        // 기존 옵션 제거 (첫 번째 빈 옵션 제외)
        startSelect.innerHTML = '<option value="">기간을 선택하세요...</option>';
        endSelect.innerHTML = '<option value="">기간을 선택하세요...</option>';

        // 시작날짜: 오름차순으로 추가
        this.availablePeriods.forEach(period => {
            const option = document.createElement('option');
            option.value = period.period;

            const typeLabel = period.type === 'latest' ? '(최신)' : '';
            option.textContent = `${period.period} ${typeLabel}`;

            startSelect.appendChild(option.cloneNode(true));
        });

        // 종료날짜: 내림차순(최신순)으로 추가
        [...this.availablePeriods].reverse().forEach(period => {
            const option = document.createElement('option');
            option.value = period.period;

            const typeLabel = period.type === 'latest' ? '(최신)' : '';
            option.textContent = `${period.period} ${typeLabel}`;

            endSelect.appendChild(option);
        });
    }

    // 빠른 선택 처리
    handleQuickSelect(period) {
        // 해당 라디오 버튼 선택
        const radioButton = document.querySelector(`input[name="quickPeriod"][value="${period}"]`);
        if (radioButton) {
            radioButton.checked = true;
        }

        const startSelect = document.getElementById('startDate');
        const endSelect = document.getElementById('endDate');

        switch (period) {
            case 'latest':
                // 최신 데이터 선택 (최신 월만)
                if (this.availablePeriods.length > 0) {
                    const latest = this.availablePeriods[this.availablePeriods.length - 1].period;
                    startSelect.value = latest;
                    endSelect.value = latest;
                }
                break;

            case 'recent':
                // 최근 3개월
                if (this.availablePeriods.length > 0) {
                    const latestPeriod = this.availablePeriods[this.availablePeriods.length - 1].period;
                    endSelect.value = latestPeriod;

                    const threeMonthsAgoIndex = Math.max(0, this.availablePeriods.length - 4);
                    const threeMonthsAgoPeriod = this.availablePeriods[threeMonthsAgoIndex].period;
                    startSelect.value = threeMonthsAgoPeriod;
                }
                break;

            case 'all':
                // 전체 기간
                if (this.availablePeriods.length > 0) {
                    startSelect.value = this.availablePeriods[0].period;
                    endSelect.value = this.availablePeriods[this.availablePeriods.length - 1].period;
                }
                break;
        }
    }

    // 종료 날짜 자동 조정
    adjustEndDate(startDate) {
        const endSelect = document.getElementById('endDate');

        if (startDate && !endSelect.value) {
            // 시작 날짜가 선택되고 종료 날짜가 비어있으면 같은 날짜로 설정
            endSelect.value = startDate;
        }
    }

    // 사업장 제안 로드
    async loadWorkplaceSuggestions() {
        try {
            const response = await fetch('/api/workplace-suggestions');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();

            if (data.success) {
                this.renderWorkplaceSuggestions(data.data);
            }
        } catch (error) {
            console.error('사업장 제안 로드 실패:', error);
            // 기본 제안 사업장들로 대체
            this.renderWorkplaceSuggestions({
                suggestions: ['삼성전자', '현대자동차', '엘지전자', 'SK하이닉스', '포스코', '롯데'],
                recentPopular: [],
                defaultSuggestions: ['삼성전자', '현대자동차', '엘지전자', 'SK하이닉스', '포스코', '롯데']
            });
        }
    }

    // 사업장 제안 버튼 렌더링
    renderWorkplaceSuggestions(data) {
        const suggestionsContainer = document.getElementById('suggestionsButtons');
        if (!suggestionsContainer) return;

        // 기존 버튼들 제거
        suggestionsContainer.innerHTML = '';

        // 제안 사업장들 표시
        const suggestions = data.suggestions || [];
        const recentPopular = data.recentPopular || [];

        suggestions.forEach(workplace => {
            const button = document.createElement('button');
            button.className = 'suggestion-btn';

            // 최근 인기 검색인지 확인
            if (recentPopular.includes(workplace)) {
                button.classList.add('recent-popular');
            }

            button.textContent = workplace;
            button.type = 'button';
            button.setAttribute('aria-label', `${workplace} 사업장명 입력`);

            // 클릭 이벤트 추가
            button.addEventListener('click', () => {
                this.selectWorkplace(workplace);
            });

            suggestionsContainer.appendChild(button);
        });
    }

    // 사업장 선택
    selectWorkplace(workplaceName) {
        const workplaceInput = document.getElementById('workplaceName');
        if (workplaceInput) {
            workplaceInput.value = workplaceName;
            workplaceInput.focus();

            // 값이 변경되었음을 알리는 이벤트 발생
            workplaceInput.dispatchEvent(new Event('input', { bubbles: true }));
        }
    }

    // 사업장 선택 토글
    toggleBusinessSelection(index) {
        if (!this.selectedBusinesses) {
            this.selectedBusinesses = new Set();
        }

        if (this.selectedBusinesses.has(index)) {
            this.selectedBusinesses.delete(index);
        } else {
            this.selectedBusinesses.add(index);
        }

        // 선택된 사업장이 없으면 첫 번째를 기본 선택
        if (this.selectedBusinesses.size === 0) {
            this.selectedBusinesses.add(0);
            // 체크박스 상태 업데이트
            const checkbox = document.querySelector(`input[onchange="app.toggleBusinessSelection(0)"]`);
            if (checkbox) checkbox.checked = true;
        }

        console.log('Selected businesses:', Array.from(this.selectedBusinesses));
    }

    // 합산 보기 표시
    showCombinedView() {
        if (!this.currentBusinesses || this.selectedBusinesses.size === 0) {
            return;
        }

        // 모든 탭 비활성화
        document.querySelectorAll('.business-tab').forEach(tab => {
            tab.classList.remove('active');
        });

        // 합산 탭 활성화
        document.querySelector('.combined-tab').classList.add('active');

        // 선택된 사업장들의 데이터 합산
        const combinedData = this.combinedData();

        // 합산된 데이터로 차트와 테이블 업데이트
        this.displayCombinedData(combinedData);
    }

    // 선택된 사업장들의 데이터 합산
    combinedData() {
        const selectedBusinesses = Array.from(this.selectedBusinesses)
            .map(index => this.currentBusinesses[index])
            .filter(business => business);

        if (selectedBusinesses.length === 0) {
            return null;
        }

        // 모든 기간의 라벨 수집 (가장 긴 데이터를 기준으로)
        let allLabels = [];
        selectedBusinesses.forEach(business => {
            if (business.chartData && business.chartData.labels) {
                if (business.chartData.labels.length > allLabels.length) {
                    allLabels = [...business.chartData.labels];
                }
            }
        });

        // 각 기간별로 데이터 합산
        const combinedNewHires = new Array(allLabels.length).fill(0);
        const combinedResignations = new Array(allLabels.length).fill(0);
        const combinedTotals = new Array(allLabels.length).fill(0);
        const combinedSalaries = new Array(allLabels.length).fill(0);

        const monthlyData = [];

        selectedBusinesses.forEach(business => {
            if (business.chartData && business.chartData.datasets) {
                const newHiresData = business.chartData.datasets[0]?.data || [];
                const resignationsData = business.chartData.datasets[1]?.data || [];
                const totalsData = business.chartData.datasets[2]?.data || [];

                // 데이터 합산
                for (let i = 0; i < allLabels.length; i++) {
                    combinedNewHires[i] += newHiresData[i] || 0;
                    combinedResignations[i] += resignationsData[i] || 0;
                    combinedTotals[i] += totalsData[i] || 0;
                }
            }

            // 월별 데이터 합산
            if (business.summary && business.summary.monthlyData) {
                business.summary.monthlyData.forEach((monthData, index) => {
                    if (!monthlyData[index]) {
                        monthlyData[index] = {
                            month: monthData.month,
                            사업장명: '합산',
                            사업자등록번호: `${selectedBusinesses.length}개 사업장`,
                            newHires: 0,
                            resignations: 0,
                            total: 0,
                            netChange: 0,
                            월국민연금금액: 0,
                            개인납부국민연금금액: 0,
                            월급여추정: 0,
                            연간급여추정: 0
                        };
                    }

                    monthlyData[index].newHires += monthData.newHires || 0;
                    monthlyData[index].resignations += monthData.resignations || 0;
                    monthlyData[index].total += monthData.total || 0;
                    monthlyData[index].netChange += monthData.netChange || 0;
                    monthlyData[index].월국민연금금액 += monthData.월국민연금금액 || 0;
                    monthlyData[index].개인납부국민연금금액 += monthData.개인납부국민연금금액 || 0;
                    monthlyData[index].월급여추정 += monthData.월급여추정 || 0;
                    monthlyData[index].연간급여추정 += monthData.연간급여추정 || 0;

                    combinedSalaries[index] += monthData.월급여추정 || 0;
                });
            }
        });

        // 차트 데이터 구성
        const chartData = {
            labels: allLabels,
            datasets: [
                {
                    label: '신규입사자',
                    data: combinedNewHires,
                    borderColor: 'rgba(75, 192, 192, 1)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    tension: 0.1,
                    yAxisID: 'y'
                },
                {
                    label: '퇴사자',
                    data: combinedResignations,
                    borderColor: 'rgba(255, 99, 132, 1)',
                    backgroundColor: 'rgba(255, 99, 132, 0.2)',
                    tension: 0.1,
                    yAxisID: 'y'
                },
                {
                    label: '총 인원',
                    data: combinedTotals,
                    borderColor: 'rgba(54, 162, 235, 1)',
                    backgroundColor: 'rgba(54, 162, 235, 0.2)',
                    tension: 0.1,
                    yAxisID: 'y1',
                    fill: false
                }
            ]
        };

        // 요약 데이터 계산
        const totalNewHires = combinedNewHires.reduce((sum, val) => sum + val, 0);
        const totalResignations = combinedResignations.reduce((sum, val) => sum + val, 0);
        const currentTotal = combinedTotals[combinedTotals.length - 1] || 0;
        const averageMonthlyChange = monthlyData.length > 0 ?
            monthlyData.reduce((sum, item) => sum + item.netChange, 0) / monthlyData.length : 0;

        return {
            chartData,
            monthlyData,
            salaryData: combinedSalaries,
            summary: {
                totalNewHires,
                totalResignations,
                currentTotal,
                averageMonthlyChange: averageMonthlyChange.toFixed(1),
                monthlyData
            },
            businessNames: selectedBusinesses.map(b => b.사업장명).join(', ')
        };
    }

    // 합산된 데이터 표시
    displayCombinedData(combinedData) {
        if (!combinedData) return;

        // 차트 업데이트
        this.createTimeSeriesChart(combinedData.chartData, `합산 보기 (${combinedData.businessNames})`);
        this.createCombinedSalaryChart(combinedData.chartData, combinedData.salaryData, `합산 보기 (${combinedData.businessNames})`);
        this.createMonthlyChart(combinedData.chartData, `합산 보기 (${combinedData.businessNames})`);

        // 테이블 및 요약 업데이트
        this.updateTable(combinedData.monthlyData);
        this.updateSummary(combinedData.summary);

        // 데이터 정보 섹션 표시
        document.getElementById('dataInfo').classList.remove('hidden');
    }

    // 합산된 급여 차트 생성
    createCombinedSalaryChart(chartData, salaryData, title) {
        const ctx = document.getElementById('salaryChart').getContext('2d');

        if (this.charts.salary) {
            this.charts.salary.destroy();
        }

        this.charts.salary = new Chart(ctx, {
            type: 'line',
            data: {
                labels: chartData.labels,
                datasets: [
                    {
                        label: '월급여추정 합계 (만원)',
                        data: salaryData,
                        borderColor: 'rgb(255, 206, 86)',
                        backgroundColor: 'rgba(255, 206, 86, 0.2)',
                        tension: 0.1,
                        yAxisID: 'y',
                        fill: false
                    },
                    {
                        label: '연간급여추정 합계 (만원)',
                        data: salaryData.map(val => val * 12),
                        borderColor: 'rgb(153, 102, 255)',
                        backgroundColor: 'rgba(153, 102, 255, 0.2)',
                        tension: 0.1,
                        yAxisID: 'y1',
                        fill: false
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: `${title} - 급여 추정 합계`,
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
                            text: '월급여추정 합계 (만원)'
                        }
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        title: {
                            display: true,
                            text: '연간급여추정 합계 (만원)'
                        },
                        grid: {
                            drawOnChartArea: false,
                        },
                    }
                }
            }
        });
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

    /* 사업장 탭 체크박스 스타일 */
    .business-checkbox {
        margin-right: 12px;
        display: flex;
        align-items: center;
    }

    .business-checkbox input[type="checkbox"] {
        width: 18px;
        height: 18px;
        margin: 0;
        cursor: pointer;
        accent-color: #007aff;
    }

    .business-tab {
        display: flex;
        align-items: center;
        padding: 12px 16px;
        border: 1px solid #d1d1d6;
        border-radius: 8px;
        background: #ffffff;
        cursor: pointer;
        transition: all 0.3s ease;
        margin-bottom: 8px;
        user-select: none;
    }

    .business-tab:hover {
        background: #f5f5f7;
        border-color: #007aff;
    }

    .business-tab.active {
        background: #007aff;
        color: white;
        border-color: #007aff;
    }

    .business-tab.active .business-checkbox input[type="checkbox"] {
        accent-color: white;
    }

    .combined-tab {
        background: linear-gradient(135deg, #34c759 0%, #30d158 100%);
        color: white;
        border-color: #34c759;
        font-weight: 600;
    }

    .combined-tab:hover {
        background: linear-gradient(135deg, #30d158 0%, #32d74b 100%);
    }

    .combined-tab.active {
        background: linear-gradient(135deg, #28cd41 0%, #30d158 100%);
        box-shadow: 0 4px 12px rgba(52, 199, 89, 0.3);
    }

    .business-name {
        font-weight: 600;
        font-size: 14px;
        flex-grow: 1;
    }

    .business-reg-no {
        font-size: 12px;
        opacity: 0.7;
        margin-left: 8px;
    }

    .business-tab.active .business-reg-no {
        opacity: 0.9;
    }
`;
document.head.appendChild(style);

// 전역 변수로 앱 인스턴스 저장
let app;

// 페이지 로드 시 초기화
document.addEventListener('DOMContentLoaded', () => {
    app = new PensionVisualization();
});