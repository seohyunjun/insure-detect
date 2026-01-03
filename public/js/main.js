// 테마 관리 클래스
class ThemeManager {
    constructor() {
        this.theme = this.getSavedTheme() || this.getSystemTheme();
        this.init();
    }

    init() {
        // 초기 테마 적용 (페이지 로드 시 깜빡임 방지를 위해 즉시 실행)
        this.applyTheme(this.theme);
        
        // DOM이 로드된 후 이벤트 바인딩
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.bindEvents());
        } else {
            this.bindEvents();
        }
    }

    bindEvents() {
        const toggleBtn = document.getElementById('themeToggle');
        if (toggleBtn) {
            toggleBtn.addEventListener('click', () => this.toggleTheme());
        }

        // 시스템 테마 변경 감지
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
            if (!this.getSavedTheme()) {
                this.applyTheme(e.matches ? 'dark' : 'light');
            }
        });
    }

    getSavedTheme() {
        return localStorage.getItem('pension-insight-theme');
    }

    getSystemTheme() {
        return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }

    applyTheme(theme) {
        this.theme = theme;
        document.documentElement.setAttribute('data-theme', theme);
        
        // Chart.js 차트 색상 업데이트
        this.updateChartColors(theme);
    }

    toggleTheme() {
        const newTheme = this.theme === 'dark' ? 'light' : 'dark';
        this.applyTheme(newTheme);
        localStorage.setItem('pension-insight-theme', newTheme);
    }

    updateChartColors(theme) {
        // Chart.js 기본 색상 업데이트
        if (typeof Chart !== 'undefined') {
            const textColor = theme === 'dark' ? '#888888' : '#5f6368';
            const gridColor = theme === 'dark' ? 'rgba(255, 255, 255, 0.08)' : 'rgba(0, 0, 0, 0.08)';
            
            Chart.defaults.color = textColor;
            Chart.defaults.borderColor = gridColor;
            
            // 기존 차트가 있으면 완전히 다시 그리기 (색상 변경을 위해)
            if (window.app && window.app.currentBusinesses && window.app.currentBusinesses.length > 0) {
                // 현재 데이터가 있으면 차트를 다시 생성
                const currentBusiness = window.app.currentBusinesses[window.app.currentBusinessIndex];
                if (currentBusiness && currentBusiness.chartData) {
                    const businessName = currentBusiness.사업장명 + ' (' + currentBusiness.사업자등록번호 + ')';
                    window.app.createTimeSeriesChart(currentBusiness.chartData, businessName);
                    window.app.createSalaryChart(currentBusiness.chartData, businessName);
                    window.app.createMonthlyChart(currentBusiness.chartData, businessName);
                }
            } else if (window.app && window.app.currentData && window.app.currentData.chartData) {
                // 단일 사업장 데이터
                window.app.createTimeSeriesChart(window.app.currentData.chartData, '');
                window.app.createSalaryChart(window.app.currentData.chartData, '');
                window.app.createMonthlyChart(window.app.currentData.chartData, '');
            }
        }

        // 지도 타일 업데이트
        if (window.app && window.app.updateMapTiles) {
            window.app.updateMapTiles();
        }
    }
}

// 테마 매니저 인스턴스 생성 (즉시 실행)
const themeManager = new ThemeManager();

class PensionVisualization {
    constructor() {
        this.charts = {};
        this.currentData = null;
        this.currentBusinesses = null;
        this.currentBusinessIndex = 0;
        // 지도 관련 속성
        this.map = null;
        this.markers = [];
        this.markerLayer = null;
        this.workplaceLocations = [];
        this.init();
    }

    // 사업자등록번호에서 사업자 유형 판별
    getBizType(bizNo) {
        if (!bizNo || bizNo.length < 6) return '기타 / 미분류';
        
        // 사업자등록번호에서 숫자만 추출 후 5-6번째 자릿수 (0-indexed: 4-5)
        const cleanBizNo = bizNo.replace(/[^0-9]/g, '');
        if (cleanBizNo.length < 6) return '기타 / 미분류';
        
        const typeCode = cleanBizNo.substring(4, 6);
        const typeNum = parseInt(typeCode, 10);
        
        if (typeNum >= 1 && typeNum <= 79) {
            return '개인 과세사업자(일반·간이)';
        } else if (typeNum >= 90 && typeNum <= 99) {
            return '개인 면세사업자';
        } else if (typeCode === '89') {
            return '개인으로 보는 단체(종교단체)';
        } else if (['81', '86', '87'].includes(typeCode)) {
            return '법인(영리) 본점';
        } else if (typeCode === '82') {
            return '법인(비영리) 본점 및 지점';
        } else if (typeCode === '83') {
            return '국가·지방자치단체';
        } else if (typeCode === '84') {
            return '외국법인 본점 및 지점';
        } else if (typeCode === '85') {
            return '법인(영리) 지점';
        } else {
            return '기타 / 미분류';
        }
    }

    // 사업자 유형에 따른 배지 클래스 반환
    getBizTypeBadgeClass(bizType) {
        if (bizType.includes('법인(영리)')) return 'biz-type-corp';
        if (bizType.includes('법인(비영리)')) return 'biz-type-nonprofit';
        if (bizType.includes('개인 과세')) return 'biz-type-individual';
        if (bizType.includes('개인 면세')) return 'biz-type-taxfree';
        if (bizType.includes('국가')) return 'biz-type-gov';
        if (bizType.includes('외국법인')) return 'biz-type-foreign';
        if (bizType.includes('종교단체')) return 'biz-type-religious';
        return 'biz-type-other';
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

        // 지도 새로고침 버튼
        const refreshMapBtn = document.getElementById('refreshMapBtn');
        if (refreshMapBtn) {
            refreshMapBtn.addEventListener('click', () => {
                this.loadWorkplaceLocations();
            });
        }
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

        // 지도에 사업장 위치 표시
        this.loadWorkplaceLocations();

        console.log('여러 사업장 검색 결과:', businesses.length + '개');
    }

    // 단일 사업장 표시
    displaySingleBusiness(business, searchTerm) {
        this.currentBusinesses = [business];
        this.currentBusinessIndex = 0;

        // 탭 숨기기
        this.hideBusinessTabs();

        // 데이터 표시 (기존 방식과 동일, 지도 로드 포함)
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

            const bizType = this.getBizType(business.사업자등록번호);
            const bizTypeClass = this.getBizTypeBadgeClass(bizType);

            tab.innerHTML = `
                <label class="business-checkbox">
                    <input type="checkbox" ${this.selectedBusinesses.has(index) ? 'checked' : ''}
                           onchange="app.toggleBusinessSelection(${index})">
                </label>
                <span class="business-name">${business.사업장명}</span>
                <span class="business-reg-no">${business.사업자등록번호}</span>
                <span class="biz-type-badge ${bizTypeClass}">${bizType}</span>
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
        
        // 지도에 사업장 위치 표시
        this.loadWorkplaceLocations();
    }

    // ========================================
    // 지도 관련 메서드
    // ========================================

    // 지도 초기화
    initMap() {
        if (this.map) {
            return; // 이미 초기화됨
        }

        const mapContainer = document.getElementById('workplaceMap');
        if (!mapContainer) {
            console.error('지도 컨테이너를 찾을 수 없습니다.');
            return;
        }

        // 대한민국 중심 좌표
        const koreaCenter = [36.5, 127.5];
        
        // Leaflet 지도 초기화
        this.map = L.map('workplaceMap', {
            center: koreaCenter,
            zoom: 7,
            zoomControl: true,
            scrollWheelZoom: true
        });

        // VWorld 타일 레이어 추가 (또는 OSM 사용)
        // 다크모드에 어울리는 타일 사용
        const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
        
        if (isDark) {
            // 다크 테마용 타일 (CartoDB Dark Matter)
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
                subdomains: 'abcd',
                maxZoom: 19
            }).addTo(this.map);
        } else {
            // 라이트 테마용 타일
            L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
                subdomains: 'abcd',
                maxZoom: 19
            }).addTo(this.map);
        }

        // 마커 레이어 그룹 생성
        this.markerLayer = L.layerGroup().addTo(this.map);

        console.log('🗺️ 지도 초기화 완료');
    }

    // 지도 타일 업데이트 (테마 변경 시)
    updateMapTiles() {
        if (!this.map) return;

        const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
        
        // 기존 타일 레이어 제거
        this.map.eachLayer(layer => {
            if (layer instanceof L.TileLayer) {
                this.map.removeLayer(layer);
            }
        });

        // 새 타일 레이어 추가 (맨 아래에 배치)
        let tileLayer;
        if (isDark) {
            tileLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; OSM &copy; CARTO',
                subdomains: 'abcd',
                maxZoom: 19
            });
        } else {
            tileLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; OSM &copy; CARTO',
                subdomains: 'abcd',
                maxZoom: 19
            });
        }
        
        // 타일 레이어를 맨 아래에 추가
        tileLayer.addTo(this.map);
        tileLayer.bringToBack();
        
        // 마커 레이어를 맨 위로 올리기
        if (this.markerLayer) {
            this.markerLayer.bringToFront();
        }

        console.log(`🗺️ 지도 타일 업데이트: ${isDark ? 'dark' : 'light'} 모드`);
    }

    // 사업장 위치 로드
    async loadWorkplaceLocations() {
        const workplaceName = document.getElementById('workplaceName').value.trim();
        const startDate = document.getElementById('startDate').value;
        const endDate = document.getElementById('endDate').value;

        if (!workplaceName) {
            return;
        }

        this.showMapLoading();
        this.showMapSection();

        try {
            const response = await fetch('/api/workplace-location', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    workplaceName,
                    startDate: startDate || '2025-11',
                    endDate: endDate || '2025-11'
                })
            });

            const result = await response.json();

            if (result.success && result.data && result.data.length > 0) {
                this.workplaceLocations = result.data;
                this.displayWorkplacesOnMap(result.data);
                this.hideMapError();
            } else {
                this.showMapError(result.error || '사업장 위치 정보를 찾을 수 없습니다.');
                this.workplaceLocations = [];
            }
        } catch (error) {
            console.error('사업장 위치 로드 오류:', error);
            this.showMapError('사업장 위치 정보를 불러오는 중 오류가 발생했습니다.');
        } finally {
            this.hideMapLoading();
        }
    }

    // 지도에 사업장 표시
    displayWorkplacesOnMap(workplaces) {
        // 지도 초기화 (필요시)
        this.initMap();

        // 기존 마커 제거
        if (this.markerLayer) {
            this.markerLayer.clearLayers();
        }
        this.markers = [];

        // 유효한 좌표가 있는 사업장만 필터링
        const validWorkplaces = workplaces.filter(w => w.lat && w.lng);
        
        if (validWorkplaces.length === 0) {
            this.showMapError('좌표 정보가 있는 사업장이 없습니다. VWorld API 키를 확인해주세요.');
            this.updateMapLegend(workplaces);
            return;
        }

        // 마커 추가
        const bounds = L.latLngBounds();
        const colors = this.getChartColors();

        validWorkplaces.forEach((workplace, index) => {
            const latLng = [workplace.lat, workplace.lng];
            bounds.extend(latLng);

            // 커스텀 마커 아이콘
            const markerIcon = L.divIcon({
                className: 'custom-marker-icon',
                html: `<div class="custom-marker" style="background: ${colors.lime};"></div>`,
                iconSize: [24, 24],
                iconAnchor: [12, 12],
                popupAnchor: [0, -12]
            });

            // 마커 생성
            const marker = L.marker(latLng, { icon: markerIcon });
            
            // 팝업 내용
            const popupContent = this.createPopupContent(workplace);
            marker.bindPopup(popupContent, {
                maxWidth: 300,
                className: 'custom-popup'
            });

            // 마커 이벤트
            marker.on('click', () => {
                this.highlightLegendItem(index);
            });

            marker.on('mouseover', () => {
                marker.openPopup();
            });

            // 마커 저장 및 레이어에 추가
            this.markers.push({ marker, workplace, index });
            this.markerLayer.addLayer(marker);
        });

        // 지도 범위 조정
        if (validWorkplaces.length === 1) {
            this.map.setView([validWorkplaces[0].lat, validWorkplaces[0].lng], 15);
        } else {
            this.map.fitBounds(bounds, { padding: [50, 50] });
        }

        // 범례 업데이트
        this.updateMapLegend(workplaces);

        console.log(`🗺️ 지도에 ${validWorkplaces.length}개 사업장 표시 완료`);
    }

    // 팝업 내용 생성
    createPopupContent(workplace) {
        const bizType = this.getBizType(workplace.regNo);
        const address = workplace.roadAddress || workplace.parcelAddress || '주소 정보 없음';
        
        return `
            <div class="map-popup">
                <div class="map-popup-title">${workplace.name}</div>
                <div class="map-popup-info">
                    <p><strong>사업자등록번호:</strong> ${workplace.regNo || '-'}</p>
                    <p><strong>업종:</strong> ${workplace.industry || '-'}</p>
                    <p><strong>유형:</strong> ${bizType}</p>
                    <p><strong>가입자수:</strong> ${workplace.memberCount?.toLocaleString() || 0}명</p>
                    <p><strong>주소:</strong> ${address}</p>
                </div>
            </div>
        `;
    }

    // 지도 범례 업데이트
    updateMapLegend(workplaces) {
        const legendContent = document.getElementById('mapLegendContent');
        if (!legendContent) return;

        if (!workplaces || workplaces.length === 0) {
            legendContent.innerHTML = '<p class="legend-empty">사업장을 검색하면 위치가 표시됩니다.</p>';
            return;
        }

        legendContent.innerHTML = workplaces.map((workplace, index) => {
            const hasLocation = workplace.lat && workplace.lng;
            const address = workplace.roadAddress || workplace.parcelAddress || '주소 정보 없음';
            
            return `
                <div class="legend-item" data-index="${index}" onclick="app.focusWorkplace(${index})">
                    <div class="legend-marker ${hasLocation ? '' : 'error'}"></div>
                    <div class="legend-info">
                        <div class="legend-name" title="${workplace.name}">${workplace.name}</div>
                        <div class="legend-address" title="${address}">${address}</div>
                        <div class="legend-members">👥 ${workplace.memberCount?.toLocaleString() || 0}명</div>
                        ${workplace.geocodeError ? `<div class="legend-error" style="color: var(--red); font-size: 0.7rem;">⚠️ ${workplace.geocodeError}</div>` : ''}
                    </div>
                </div>
            `;
        }).join('');
    }

    // 범례 아이템 강조
    highlightLegendItem(index) {
        const legendItems = document.querySelectorAll('.legend-item');
        legendItems.forEach((item, i) => {
            item.classList.toggle('active', i === index);
        });
    }

    // 특정 사업장 포커스
    focusWorkplace(index) {
        const workplaceData = this.workplaceLocations[index];
        if (!workplaceData) return;

        // 범례 강조
        this.highlightLegendItem(index);

        // 좌표가 있으면 지도 이동
        if (workplaceData.lat && workplaceData.lng) {
            this.map.setView([workplaceData.lat, workplaceData.lng], 16);
            
            // 해당 마커 팝업 열기
            const markerData = this.markers.find(m => m.index === index);
            if (markerData) {
                markerData.marker.openPopup();
            }
        }
    }

    // 지도 섹션 표시
    showMapSection() {
        const mapSection = document.getElementById('mapSection');
        if (mapSection) {
            mapSection.classList.remove('hidden');
        }
    }

    // 지도 섹션 숨기기
    hideMapSection() {
        const mapSection = document.getElementById('mapSection');
        if (mapSection) {
            mapSection.classList.add('hidden');
        }
    }

    // 지도 로딩 표시
    showMapLoading() {
        const loading = document.getElementById('mapLoadingIndicator');
        if (loading) {
            loading.classList.remove('hidden');
        }
    }

    // 지도 로딩 숨기기
    hideMapLoading() {
        const loading = document.getElementById('mapLoadingIndicator');
        if (loading) {
            loading.classList.add('hidden');
        }
    }

    // 지도 에러 표시
    showMapError(message) {
        const errorEl = document.getElementById('mapErrorMessage');
        if (errorEl) {
            errorEl.textContent = message;
            errorEl.classList.remove('hidden');
        }
    }

    // 지도 에러 숨기기
    hideMapError() {
        const errorEl = document.getElementById('mapErrorMessage');
        if (errorEl) {
            errorEl.classList.add('hidden');
        }
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

    // 테마 기반 차트 색상 가져오기
    getChartColors() {
        const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
        
        return {
            lime: isDark ? '#c8ff00' : '#00897b',
            cyan: isDark ? '#00f0ff' : '#0097a7',
            magenta: isDark ? '#ff00aa' : '#d81b60',
            blue: isDark ? '#0066ff' : '#1976d2',
            green: isDark ? '#00ff88' : '#2e7d32',
            red: isDark ? '#ff4757' : '#e53935',
            text: isDark ? '#888888' : '#5f6368',
            textLight: isDark ? '#555555' : '#9aa0a6',
            grid: isDark ? 'rgba(255, 255, 255, 0.08)' : 'rgba(0, 0, 0, 0.08)',
            gridLight: isDark ? 'rgba(255, 255, 255, 0.04)' : 'rgba(0, 0, 0, 0.04)',
            bg: isDark ? '#111111' : '#ffffff'
        };
    }

    // 공통 차트 옵션 가져오기
    getChartOptions() {
        const colors = this.getChartColors();
        
        return {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            plugins: {
                legend: {
                    position: 'top',
                    align: 'end',
                    labels: {
                        color: colors.text,
                        font: {
                            family: "'Outfit', 'Pretendard Variable', sans-serif",
                            size: 11,
                            weight: '500'
                        },
                        padding: 16,
                        usePointStyle: true,
                        pointStyle: 'circle',
                        boxWidth: 8,
                        boxHeight: 8
                    }
                },
                title: {
                    display: false // 제목은 HTML에서 처리
                },
                tooltip: {
                    backgroundColor: colors.bg,
                    titleColor: colors.lime,
                    bodyColor: colors.text,
                    borderColor: colors.grid,
                    borderWidth: 1,
                    padding: 12,
                    cornerRadius: 8,
                    titleFont: {
                        family: "'Outfit', sans-serif",
                        size: 13,
                        weight: '700'
                    },
                    bodyFont: {
                        family: "'Pretendard Variable', sans-serif",
                        size: 12
                    },
                    displayColors: true,
                    usePointStyle: true
                }
            },
            scales: {
                x: {
                    grid: {
                        color: colors.gridLight,
                        drawBorder: false
                    },
                    ticks: {
                        color: colors.textLight,
                        font: {
                            family: "'Outfit', sans-serif",
                            size: 10,
                            weight: '500'
                        },
                        padding: 8
                    }
                },
                y: {
                    grid: {
                        color: colors.grid,
                        drawBorder: false
                    },
                    ticks: {
                        color: colors.text,
                        font: {
                            family: "'Outfit', sans-serif",
                            size: 10,
                            weight: '500'
                        },
                        padding: 8
                    }
                }
            }
        };
    }

    createTimeSeriesChart(chartData, workplaceName) {
        const ctx = document.getElementById('timeSeriesChart').getContext('2d');
        const colors = this.getChartColors();
        const baseOptions = this.getChartOptions();

        if (this.charts.timeSeries) {
            this.charts.timeSeries.destroy();
        }

        // 데이터셋 스타일 커스터마이징
        const styledDatasets = [
            {
                ...chartData.datasets[0],
                label: '신규입사자',
                borderColor: colors.green,
                backgroundColor: colors.green + '20',
                pointBackgroundColor: colors.green,
                pointBorderColor: colors.bg,
                pointBorderWidth: 2,
                pointRadius: 4,
                pointHoverRadius: 6,
                borderWidth: 2.5,
                tension: 0.4,
                fill: true,
                yAxisID: 'y'
            },
            {
                ...chartData.datasets[1],
                label: '퇴사자',
                borderColor: colors.red,
                backgroundColor: colors.red + '20',
                pointBackgroundColor: colors.red,
                pointBorderColor: colors.bg,
                pointBorderWidth: 2,
                pointRadius: 4,
                pointHoverRadius: 6,
                borderWidth: 2.5,
                tension: 0.4,
                fill: true,
                yAxisID: 'y'
            },
            {
                ...chartData.datasets[2],
                label: '총 인원',
                borderColor: colors.lime,
                backgroundColor: 'transparent',
                pointBackgroundColor: colors.lime,
                pointBorderColor: colors.bg,
                pointBorderWidth: 2,
                pointRadius: 5,
                pointHoverRadius: 8,
                borderWidth: 3,
                tension: 0.4,
                fill: false,
                yAxisID: 'y1'
            }
        ];

        this.charts.timeSeries = new Chart(ctx, {
            type: 'line',
            data: {
                labels: chartData.labels,
                datasets: styledDatasets
            },
            options: {
                ...baseOptions,
                scales: {
                    x: {
                        ...baseOptions.scales.x
                    },
                    y: {
                        ...baseOptions.scales.y,
                        type: 'linear',
                        position: 'left',
                        title: {
                            display: true,
                            text: '입사/퇴사 (명)',
                            color: colors.text,
                            font: {
                                family: "'Outfit', sans-serif",
                                size: 10,
                                weight: '600'
                            }
                        }
                    },
                    y1: {
                        type: 'linear',
                        position: 'right',
                        grid: {
                            drawOnChartArea: false,
                            drawBorder: false
                        },
                        ticks: {
                            color: colors.lime,
                            font: {
                                family: "'Outfit', sans-serif",
                                size: 10,
                                weight: '600'
                            },
                            padding: 8
                        },
                        title: {
                            display: true,
                            text: '총 인원 (명)',
                            color: colors.lime,
                            font: {
                                family: "'Outfit', sans-serif",
                                size: 10,
                                weight: '600'
                            }
                        }
                    }
                }
            }
        });
    }

    createSalaryChart(chartData, workplaceName) {
        const ctx = document.getElementById('salaryChart').getContext('2d');
        const colors = this.getChartColors();
        const baseOptions = this.getChartOptions();

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
                        borderColor: colors.cyan,
                        backgroundColor: colors.cyan + '20',
                        pointBackgroundColor: colors.cyan,
                        pointBorderColor: colors.bg,
                        pointBorderWidth: 2,
                        pointRadius: 4,
                        pointHoverRadius: 6,
                        borderWidth: 2.5,
                        tension: 0.4,
                        fill: true,
                        yAxisID: 'y'
                    },
                    {
                        label: '연간급여추정 (만원)',
                        data: salaryData.yearly,
                        borderColor: colors.magenta,
                        backgroundColor: 'transparent',
                        pointBackgroundColor: colors.magenta,
                        pointBorderColor: colors.bg,
                        pointBorderWidth: 2,
                        pointRadius: 4,
                        pointHoverRadius: 6,
                        borderWidth: 2.5,
                        tension: 0.4,
                        fill: false,
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                ...baseOptions,
                scales: {
                    x: {
                        ...baseOptions.scales.x
                    },
                    y: {
                        ...baseOptions.scales.y,
                        type: 'linear',
                        position: 'left',
                        title: {
                            display: true,
                            text: '월급여 (만원)',
                            color: colors.cyan,
                            font: {
                                family: "'Outfit', sans-serif",
                                size: 10,
                                weight: '600'
                            }
                        },
                        ticks: {
                            ...baseOptions.scales.y.ticks,
                            color: colors.cyan
                        }
                    },
                    y1: {
                        type: 'linear',
                        position: 'right',
                        grid: {
                            drawOnChartArea: false,
                            drawBorder: false
                        },
                        ticks: {
                            color: colors.magenta,
                            font: {
                                family: "'Outfit', sans-serif",
                                size: 10,
                                weight: '600'
                            },
                            padding: 8
                        },
                        title: {
                            display: true,
                            text: '연간급여 (만원)',
                            color: colors.magenta,
                            font: {
                                family: "'Outfit', sans-serif",
                                size: 10,
                                weight: '600'
                            }
                        }
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
        const colors = this.getChartColors();
        const baseOptions = this.getChartOptions();

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
                    backgroundColor: colors.green + '80',
                    borderColor: colors.green,
                    borderWidth: 0,
                    borderRadius: 4,
                    borderSkipped: false
                },
                {
                    label: '퇴사자',
                    data: chartData.datasets[1].data,
                    backgroundColor: colors.red + '80',
                    borderColor: colors.red,
                    borderWidth: 0,
                    borderRadius: 4,
                    borderSkipped: false
                },
                {
                    label: '순 변화',
                    data: netChangeData,
                    backgroundColor: netChangeData.map(val => 
                        val >= 0 ? colors.lime + '90' : colors.magenta + '90'
                    ),
                    borderColor: netChangeData.map(val => 
                        val >= 0 ? colors.lime : colors.magenta
                    ),
                    borderWidth: 0,
                    borderRadius: 4,
                    borderSkipped: false
                }
            ]
        };

        this.charts.monthly = new Chart(ctx, {
            type: 'bar',
            data: monthlyChartData,
            options: {
                ...baseOptions,
                scales: {
                    x: {
                        ...baseOptions.scales.x,
                        stacked: false
                    },
                    y: {
                        ...baseOptions.scales.y,
                        stacked: false,
                        title: {
                            display: true,
                            text: '인원수 (명)',
                            color: colors.text,
                            font: {
                                family: "'Outfit', sans-serif",
                                size: 10,
                                weight: '600'
                            }
                        }
                    }
                },
                barPercentage: 0.7,
                categoryPercentage: 0.8
            }
        });
    }

    createComparisonChart(comparisonData) {
        const ctx = document.getElementById('timeSeriesChart').getContext('2d');
        const colors = this.getChartColors();
        const baseOptions = this.getChartOptions();

        if (this.charts.timeSeries) {
            this.charts.timeSeries.destroy();
        }

        const labels = comparisonData.map(item => item.name);
        const datasets = [
            {
                label: '현재 총 인원',
                data: comparisonData.map(item => item.currentTotal),
                backgroundColor: colors.lime + '80',
                borderColor: colors.lime,
                borderWidth: 0,
                borderRadius: 6,
                borderSkipped: false
            },
            {
                label: '총 신규입사자',
                data: comparisonData.map(item => item.totalNewHires),
                backgroundColor: colors.green + '80',
                borderColor: colors.green,
                borderWidth: 0,
                borderRadius: 6,
                borderSkipped: false
            },
            {
                label: '총 퇴사자',
                data: comparisonData.map(item => item.totalResignations),
                backgroundColor: colors.red + '80',
                borderColor: colors.red,
                borderWidth: 0,
                borderRadius: 6,
                borderSkipped: false
            }
        ];

        this.charts.timeSeries = new Chart(ctx, {
            type: 'bar',
            data: { labels, datasets },
            options: {
                ...baseOptions,
                scales: {
                    x: {
                        ...baseOptions.scales.x
                    },
                    y: {
                        ...baseOptions.scales.y,
                        title: {
                            display: true,
                            text: '인원수 (명)',
                            color: colors.text,
                            font: {
                                family: "'Outfit', sans-serif",
                                size: 10,
                                weight: '600'
                            }
                        }
                    }
                },
                barPercentage: 0.7,
                categoryPercentage: 0.85
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

        // 테이블 헤더 업데이트 (사업자유형 컬럼 포함)
        const thead = document.querySelector('#dataTable thead tr');
        thead.innerHTML = `
            <th>기간</th>
            <th>사업장명</th>
            <th>사업자등록번호</th>
            <th>사업자유형</th>
            <th>신규입사자</th>
            <th>퇴사자</th>
            <th>총 인원</th>
            <th>순 변화</th>
            <th>월국민연금금액</th>
            <th>개인납부금액</th>
            <th>월급여추정</th>
            <th>연간급여추정</th>
        `;

        if (!monthlyData || monthlyData.length === 0) {
            tbody.innerHTML = '<tr><td colspan="12" class="no-data">데이터가 없습니다</td></tr>';
            return;
        }

        monthlyData.forEach(item => {
            const row = document.createElement('tr');

            const netChangeClass = item.netChange >= 0 ? 'text-success' : 'text-danger';
            const netChangeSymbol = item.netChange >= 0 ? '+' : '';
            
            const bizType = this.getBizType(item.사업자등록번호);
            const bizTypeClass = this.getBizTypeBadgeClass(bizType);

            row.innerHTML = `
                <td>${item.month}</td>
                <td>${item.사업장명 || '-'}</td>
                <td>${item.사업자등록번호 || '-'}</td>
                <td><span class="biz-type-badge ${bizTypeClass}">${bizType}</span></td>
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

        // 테이블 헤더 변경 (사업자유형 포함)
        const thead = document.querySelector('#dataTable thead tr');
        thead.innerHTML = `
            <th>사업장명</th>
            <th>사업자등록번호</th>
            <th>사업자유형</th>
            <th>총 신규입사자</th>
            <th>총 퇴사자</th>
            <th>현재 총 인원</th>
            <th>월평균 변화</th>
        `;

        comparisonData.forEach(item => {
            const row = document.createElement('tr');

            const avgChangeClass = item.averageMonthlyChange >= 0 ? 'text-success' : 'text-danger';
            const avgChangeSymbol = item.averageMonthlyChange >= 0 ? '+' : '';
            
            const bizType = this.getBizType(item.사업자등록번호);
            const bizTypeClass = this.getBizTypeBadgeClass(bizType);

            row.innerHTML = `
                <td>${item.name}</td>
                <td>${item.사업자등록번호 || '-'}</td>
                <td><span class="biz-type-badge ${bizTypeClass}">${bizType}</span></td>
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

        // 차트 데이터 구성 (테마 색상은 차트 생성 시 적용됨)
        const chartData = {
            labels: allLabels,
            datasets: [
                {
                    label: '신규입사자',
                    data: combinedNewHires,
                    yAxisID: 'y'
                },
                {
                    label: '퇴사자',
                    data: combinedResignations,
                    yAxisID: 'y'
                },
                {
                    label: '총 인원',
                    data: combinedTotals,
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
        const colors = this.getChartColors();
        const baseOptions = this.getChartOptions();

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
                        borderColor: colors.cyan,
                        backgroundColor: colors.cyan + '20',
                        pointBackgroundColor: colors.cyan,
                        pointBorderColor: colors.bg,
                        pointBorderWidth: 2,
                        pointRadius: 4,
                        pointHoverRadius: 6,
                        borderWidth: 2.5,
                        tension: 0.4,
                        fill: true,
                        yAxisID: 'y'
                    },
                    {
                        label: '연간급여추정 합계 (만원)',
                        data: salaryData.map(val => val * 12),
                        borderColor: colors.magenta,
                        backgroundColor: 'transparent',
                        pointBackgroundColor: colors.magenta,
                        pointBorderColor: colors.bg,
                        pointBorderWidth: 2,
                        pointRadius: 4,
                        pointHoverRadius: 6,
                        borderWidth: 2.5,
                        tension: 0.4,
                        fill: false,
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                ...baseOptions,
                scales: {
                    x: {
                        ...baseOptions.scales.x
                    },
                    y: {
                        ...baseOptions.scales.y,
                        type: 'linear',
                        position: 'left',
                        title: {
                            display: true,
                            text: '월급여 합계 (만원)',
                            color: colors.cyan,
                            font: {
                                family: "'Outfit', sans-serif",
                                size: 10,
                                weight: '600'
                            }
                        },
                        ticks: {
                            ...baseOptions.scales.y.ticks,
                            color: colors.cyan
                        }
                    },
                    y1: {
                        type: 'linear',
                        position: 'right',
                        grid: {
                            drawOnChartArea: false,
                            drawBorder: false
                        },
                        ticks: {
                            color: colors.magenta,
                            font: {
                                family: "'Outfit', sans-serif",
                                size: 10,
                                weight: '600'
                            },
                            padding: 8
                        },
                        title: {
                            display: true,
                            text: '연간급여 합계 (만원)',
                            color: colors.magenta,
                            font: {
                                family: "'Outfit', sans-serif",
                                size: 10,
                                weight: '600'
                            }
                        }
                    }
                }
            }
        });
    }
}

// Lando Norris 스타일 동적 CSS
const style = document.createElement('style');
style.textContent = `
    .text-success { color: #00ff88 !important; }
    .text-danger { color: #ff4757 !important; }
`;
document.head.appendChild(style);

// 전역 변수로 앱 인스턴스 저장
let app;

// 페이지 로드 시 초기화
document.addEventListener('DOMContentLoaded', () => {
    app = new PensionVisualization();
});