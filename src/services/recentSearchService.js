const fs = require('fs').promises;
const path = require('path');

class RecentSearchService {
    constructor() {
        this.searchHistoryFile = path.join(__dirname, '../../source/logs/recent_searches.json');
        this.maxSearches = 50; // 최대 저장할 검색 내역 수
        this.searchHistory = [];

        // 초기화 시 기존 검색 내역 로드
        this.loadSearchHistory();
    }

    /**
     * 기존 검색 내역 로드
     */
    async loadSearchHistory() {
        try {
            // logs 디렉토리가 없으면 생성
            const logsDir = path.dirname(this.searchHistoryFile);
            try {
                await fs.access(logsDir);
            } catch {
                await fs.mkdir(logsDir, { recursive: true });
            }

            // 검색 내역 파일 로드
            const data = await fs.readFile(this.searchHistoryFile, 'utf8');
            this.searchHistory = JSON.parse(data);

            console.log(`✅ 기존 검색 내역 로드: ${this.searchHistory.length}개`);
        } catch (error) {
            // 파일이 없으면 빈 배열로 시작
            this.searchHistory = [];
            console.log('📝 새로운 검색 내역 시작');
        }
    }

    /**
     * 검색 내역 저장
     */
    async saveSearchHistory() {
        try {
            await fs.writeFile(
                this.searchHistoryFile,
                JSON.stringify(this.searchHistory, null, 2),
                'utf8'
            );
        } catch (error) {
            console.error('❌ 검색 내역 저장 실패:', error.message);
        }
    }

    /**
     * 새로운 검색 내역 추가
     */
    async addSearch(searchParams) {
        const searchEntry = {
            id: Date.now().toString(),
            timestamp: new Date().toISOString(),
            searchType: searchParams.type || 'workplace_stats', // workplace_stats, data_query, custom_sql
            parameters: {
                startDate: searchParams.startDate,
                endDate: searchParams.endDate,
                workplaceName: searchParams.workplaceName || null,
                customSQL: searchParams.customSQL || null
            },
            resultCount: searchParams.resultCount || 0,
            queryTime: searchParams.queryTime || '0',
            success: searchParams.success !== false
        };

        // 중복 검색 제거 (같은 파라미터의 검색이 있으면 제거)
        this.searchHistory = this.searchHistory.filter(search => {
            return !(
                search.searchType === searchEntry.searchType &&
                search.parameters.startDate === searchEntry.parameters.startDate &&
                search.parameters.endDate === searchEntry.parameters.endDate &&
                search.parameters.workplaceName === searchEntry.parameters.workplaceName &&
                search.parameters.customSQL === searchEntry.parameters.customSQL
            );
        });

        // 새로운 검색을 맨 앞에 추가
        this.searchHistory.unshift(searchEntry);

        // 최대 개수 제한
        if (this.searchHistory.length > this.maxSearches) {
            this.searchHistory = this.searchHistory.slice(0, this.maxSearches);
        }

        // 파일에 저장
        await this.saveSearchHistory();

        console.log(`📝 새로운 검색 내역 추가: ${searchEntry.searchType} (${searchEntry.parameters.startDate} ~ ${searchEntry.parameters.endDate})`);

        return searchEntry;
    }

    /**
     * 최근 검색 내역 조회
     */
    getRecentSearches(limit = 20) {
        return this.searchHistory.slice(0, limit);
    }

    /**
     * 검색 타입별 내역 조회
     */
    getSearchesByType(searchType, limit = 10) {
        return this.searchHistory
            .filter(search => search.searchType === searchType)
            .slice(0, limit);
    }

    /**
     * 인기 검색 조건 분석
     */
    getPopularSearches() {
        const workplaceStats = {};
        const dateRanges = {};

        this.searchHistory.forEach(search => {
            // 사업장명 통계
            if (search.parameters.workplaceName) {
                const workplace = search.parameters.workplaceName;
                workplaceStats[workplace] = (workplaceStats[workplace] || 0) + 1;
            }

            // 날짜 범위 통계
            const dateRange = `${search.parameters.startDate}_${search.parameters.endDate}`;
            dateRanges[dateRange] = (dateRanges[dateRange] || 0) + 1;
        });

        // 상위 10개씩 정렬
        const topWorkplaces = Object.entries(workplaceStats)
            .sort(([,a], [,b]) => b - a)
            .slice(0, 10)
            .map(([name, count]) => ({ workplaceName: name, searchCount: count }));

        const topDateRanges = Object.entries(dateRanges)
            .sort(([,a], [,b]) => b - a)
            .slice(0, 10)
            .map(([range, count]) => {
                const [startDate, endDate] = range.split('_');
                return { startDate, endDate, searchCount: count };
            });

        return {
            totalSearches: this.searchHistory.length,
            topWorkplaces: topWorkplaces,
            topDateRanges: topDateRanges,
            searchTypeDistribution: this.getSearchTypeDistribution()
        };
    }

    /**
     * 검색 타입별 분포
     */
    getSearchTypeDistribution() {
        const distribution = {};

        this.searchHistory.forEach(search => {
            distribution[search.searchType] = (distribution[search.searchType] || 0) + 1;
        });

        return Object.entries(distribution)
            .map(([type, count]) => ({ searchType: type, count: count }))
            .sort((a, b) => b.count - a.count);
    }

    /**
     * 특정 검색 내역 삭제
     */
    async deleteSearch(searchId) {
        const initialLength = this.searchHistory.length;
        this.searchHistory = this.searchHistory.filter(search => search.id !== searchId);

        if (this.searchHistory.length < initialLength) {
            await this.saveSearchHistory();
            console.log(`🗑️ 검색 내역 삭제: ${searchId}`);
            return true;
        }

        return false;
    }

    /**
     * 모든 검색 내역 삭제
     */
    async clearAllSearches() {
        this.searchHistory = [];
        await this.saveSearchHistory();
        console.log('🗑️ 모든 검색 내역 삭제');
    }

    /**
     * 검색 내역 요약 정보
     */
    getSearchSummary() {
        const now = new Date();
        const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        const thisWeek = new Date(today.getTime() - (7 * 24 * 60 * 60 * 1000));
        const thisMonth = new Date(today.getTime() - (30 * 24 * 60 * 60 * 1000));

        const todaySearches = this.searchHistory.filter(search =>
            new Date(search.timestamp) >= today
        ).length;

        const weekSearches = this.searchHistory.filter(search =>
            new Date(search.timestamp) >= thisWeek
        ).length;

        const monthSearches = this.searchHistory.filter(search =>
            new Date(search.timestamp) >= thisMonth
        ).length;

        const successfulSearches = this.searchHistory.filter(search =>
            search.success
        ).length;

        return {
            total: this.searchHistory.length,
            today: todaySearches,
            thisWeek: weekSearches,
            thisMonth: monthSearches,
            successRate: this.searchHistory.length > 0
                ? Math.round((successfulSearches / this.searchHistory.length) * 100)
                : 0,
            averageQueryTime: this.searchHistory.length > 0
                ? (this.searchHistory.reduce((sum, search) =>
                    sum + parseFloat(search.queryTime || 0), 0) / this.searchHistory.length).toFixed(2)
                : '0'
        };
    }
}

module.exports = RecentSearchService;