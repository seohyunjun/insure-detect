const { execSync, spawn } = require('child_process');
const fs = require('fs').promises;
const path = require('path');
const moment = require('moment');

class DuckDBQueryService {
    constructor() {
        this.sourceDir = path.join(__dirname, '../../source/data');
        this.tempDir = path.join(__dirname, '../../temp');
    }

    async initialize() {
        // temp 디렉토리가 없으면 생성
        try {
            await fs.access(this.tempDir);
        } catch {
            await fs.mkdir(this.tempDir, { recursive: true });
        }
    }

    /**
     * 기간별 parquet 파일들을 찾기
     */
    async findParquetFiles(startDate, endDate, uddiName = 'pension_workplace') {
        const files = await fs.readdir(this.sourceDir);
        const start = moment(startDate, 'YYYY-MM');
        const end = moment(endDate, 'YYYY-MM');

        const parquetFiles = files.filter(file => {
            if (!file.endsWith('.parquet')) return false;

            // pension_workplace_YYYY-MM.parquet 패턴
            if (file.startsWith(`${uddiName}_`)) {
                const match = file.match(/(\d{4}-\d{2})\.parquet$/);
                if (match) {
                    const fileDate = moment(match[1], 'YYYY-MM');
                    // isBetween 대신 >= 및 <= 연산자를 사용하여 종료 날짜 포함
                    return fileDate.isSameOrAfter(start) && fileDate.isSameOrBefore(end);
                }
            }

            // pension_YYYY-MM_YYYY-MM.parquet 패턴 (두 번째 날짜가 실제 데이터 날짜)
            if (file.startsWith('pension_')) {
                const match = file.match(/pension_\d{4}-\d{2}_(\d{4}-\d{2})\.parquet$/);
                if (match) {
                    const fileDate = moment(match[1], 'YYYY-MM');
                    // isBetween 대신 >= 및 <= 연산자를 사용하여 종료 날짜 포함
                    return fileDate.isSameOrAfter(start) && fileDate.isSameOrBefore(end);
                }
            }

            return false;
        });

        // 절대 경로로 변환
        return parquetFiles.map(file => path.join(this.sourceDir, file));
    }

    /**
     * DuckDB SQL 질의 실행
     */
    async executeQuery(sql, options = {}) {
        const startTime = Date.now();
        console.log(`🦆 DuckDB 질의 실행: ${sql.substring(0, 100)}${sql.length > 100 ? '...' : ''}`);

        try {
            await this.initialize();

            // DuckDB CLI 실행을 위한 SQL 파일 생성
            const fullSQL = sql;

            // 임시 SQL 파일 생성
            const tempSQLFile = path.join(this.tempDir, `query_${Date.now()}.sql`);
            await fs.writeFile(tempSQLFile, fullSQL);

            // DuckDB 실행 (CSV 출력으로 변경)
            const result = execSync(`duckdb -csv < "${tempSQLFile}"`, {
                encoding: 'utf8',
                maxBuffer: 50 * 1024 * 1024 // 50MB 버퍼
            });

            // 임시 파일 삭제
            await fs.unlink(tempSQLFile).catch(() => {});

            // CSV 파싱
            let data = [];
            if (result.trim()) {
                try {
                    const lines = result.trim().split('\n');
                    if (lines.length > 1) {
                        const headers = lines[0].split(',').map(h => h.replace(/"/g, ''));
                        data = lines.slice(1).map(line => {
                            const values = line.split(',').map(v => v.replace(/"/g, ''));
                            const obj = {};
                            headers.forEach((header, index) => {
                                obj[header] = values[index] || '';
                            });
                            return obj;
                        });
                    }
                } catch (parseError) {
                    console.warn('⚠️ CSV 파싱 실패, 원본 결과 반환');
                    data = [{ raw_result: result }];
                }
            }

            const endTime = Date.now();
            const queryTime = ((endTime - startTime) / 1000).toFixed(2);
            console.log(`✅ DuckDB 질의 완료: ${data.length.toLocaleString()}개 결과 (${queryTime}초)`);

            return {
                success: true,
                data: data,
                queryTime: queryTime,
                recordCount: data.length
            };

        } catch (error) {
            const endTime = Date.now();
            const queryTime = ((endTime - startTime) / 1000).toFixed(2);
            console.error(`❌ DuckDB 질의 실패 (${queryTime}초):`, error.message);

            return {
                success: false,
                error: error.message,
                queryTime: queryTime,
                data: []
            };
        }
    }

    /**
     * 기간별 데이터를 SQL로 질의
     */
    async queryDataByDateRange(startDate, endDate, workplaceNameFilter = null, uddiName = 'pension_workplace') {
        const startTime = Date.now();
        console.log(`🔍 기간별 DuckDB 질의: ${startDate} ~ ${endDate}`);

        try {
            // 파일 찾기
            const files = await this.findParquetFiles(startDate, endDate, uddiName);

            if (files.length === 0) {
                return {
                    success: false,
                    error: `기간 ${startDate} ~ ${endDate} 내의 parquet 파일을 찾을 수 없습니다.`
                };
            }

            console.log(`📁 발견된 파일: ${files.length}개`);

            // SQL 질의 구성
            const filePatterns = files.map(file => `'${file}'`).join(', ');

            let sql = `
                SELECT *
                FROM read_parquet([${filePatterns}])
            `;

            // WHERE 조건 추가
            const conditions = [];

            // 사업장명 또는 사업자등록번호 필터링
            if (workplaceNameFilter) {
                const escapedFilter = workplaceNameFilter.replace(/'/g, "''");
                // 숫자만 포함된 경우 사업자등록번호로 간주
                if (/^\d+$/.test(workplaceNameFilter)) {
                    conditions.push(`사업자등록번호 LIKE '%${escapedFilter}%'`);
                } else {
                    conditions.push(`사업장명 LIKE '%${escapedFilter}%'`);
                }
            }

            // 날짜 범위 필터링 (데이터에 dash가 포함되어 있음)
            conditions.push(`자료생성년월 >= '${startDate}' AND 자료생성년월 <= '${endDate}'`);

            if (conditions.length > 0) {
                sql += ` WHERE ${conditions.join(' AND ')}`;
            }

            // 정렬 추가
            sql += ` ORDER BY 자료생성년월, 사업장명`;

            // SQL 질의 실행
            const result = await this.executeQuery(sql);

            if (result.success) {
                const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);

                return {
                    success: true,
                    data: result.data,
                    metadata: {
                        uddiName: uddiName,
                        totalRecords: result.data.length,
                        dateRange: { startDate, endDate },
                        filesCount: files.length,
                        workplaceNameFilter: workplaceNameFilter,
                        loadMethod: 'DuckDB SQL Query',
                        queryTime: result.queryTime,
                        totalTime: totalTime
                    },
                    filesLoaded: files.length,
                    totalLoadTime: totalTime,
                    method: 'DuckDB'
                };
            } else {
                return result;
            }

        } catch (error) {
            const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
            console.error(`❌ 기간별 DuckDB 질의 실패 (${totalTime}초):`, error.message);

            return {
                success: false,
                error: `DuckDB 질의 중 오류: ${error.message}`,
                totalTime: totalTime
            };
        }
    }

    /**
     * 사업장별 통계 질의
     */
    async getWorkplaceStats(startDate, endDate, workplaceNameFilter = null) {
        console.log(`📊 사업장 통계 질의: ${startDate} ~ ${endDate}`);

        try {
            const files = await this.findParquetFiles(startDate, endDate);

            if (files.length === 0) {
                return { success: false, error: '파일을 찾을 수 없습니다.' };
            }

            const filePatterns = files.map(file => `'${file}'`).join(', ');

            let sql = `
                SELECT
                    사업장명,
                    사업자등록번호,
                    COUNT(*) as 총_레코드수,
                    SUM(CAST(가입자수 AS INTEGER)) as 총_가입자수,
                    SUM(CAST(신규취득자수 AS INTEGER)) as 총_신규취득자수,
                    SUM(CAST(상실가입자수 AS INTEGER)) as 총_상실가입자수,
                    MIN(자료생성년월) as 최초_년월,
                    MAX(자료생성년월) as 최종_년월
                FROM read_parquet([${filePatterns}])
            `;

            if (workplaceNameFilter) {
                const escapedFilter = workplaceNameFilter.replace(/'/g, "''");
                // 숫자만 포함된 경우 사업자등록번호로 간주
                if (/^\d+$/.test(workplaceNameFilter)) {
                    sql += ` WHERE 사업자등록번호 LIKE '%${escapedFilter}%'`;
                } else {
                    sql += ` WHERE 사업장명 LIKE '%${escapedFilter}%'`;
                }
            }

            sql += `
                GROUP BY 사업장명, 사업자등록번호
                ORDER BY 총_가입자수 DESC
                LIMIT 100
            `;

            return await this.executeQuery(sql);

        } catch (error) {
            console.error('❌ 사업장 통계 질의 실패:', error.message);
            return {
                success: false,
                error: error.message,
                data: []
            };
        }
    }

    /**
     * 커스텀 SQL 질의 (고급 사용자용)
     */
    async executeCustomQuery(customSQL, startDate = null, endDate = null) {
        console.log(`🔧 커스텀 SQL 질의 실행`);

        try {
            // 날짜 범위가 지정된 경우 파일 패턴 치환
            if (startDate && endDate) {
                const files = await this.findParquetFiles(startDate, endDate);
                const filePatterns = files.map(file => `'${file}'`).join(', ');

                // {{FILES}} 플레이스홀더를 실제 파일 경로로 치환
                customSQL = customSQL.replace(/\{\{FILES\}\}/g, `[${filePatterns}]`);
            }

            return await this.executeQuery(customSQL);

        } catch (error) {
            console.error('❌ 커스텀 SQL 질의 실패:', error.message);
            return {
                success: false,
                error: error.message,
                data: []
            };
        }
    }

    /**
     * 메모리 사용량 확인
     */
    getMemoryUsage() {
        const usage = process.memoryUsage();
        return {
            usedMB: Math.round(usage.heapUsed / 1024 / 1024),
            totalMB: Math.round(usage.heapTotal / 1024 / 1024),
            rssMB: Math.round(usage.rss / 1024 / 1024)
        };
    }
}

module.exports = DuckDBQueryService;