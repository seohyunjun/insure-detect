#!/usr/bin/env node

const fs = require('fs').promises;
const path = require('path');
const DataCollector = require('../src/services/dataCollector');

class UDDICollector {
    constructor() {
        this.dataCollector = new DataCollector();
        this.availableCommands = {
            'list': 'UDDI 목록 보기',
            'collect': 'UDDI 데이터 수집',
            'load': '저장된 데이터 로드',
            'status': '수집된 데이터 상태 확인',
            'query': '사업장별 데이터 조회',
            'date': '지정된 날짜의 UDDI 데이터 수집/조회',
            'uddi': 'UDDI 식별자와 날짜로 데이터 수집',
            'help': '도움말 보기'
        };
    }

    async init() {
        const args = process.argv.slice(2);

        if (args.length === 0) {
            this.showHelp();
            return;
        }

        const command = args[0];
        const params = args.slice(1);

        try {
            switch (command) {
                case 'list':
                    await this.listUddis();
                    break;
                case 'collect':
                    await this.collectData(params);
                    break;
                case 'load':
                    await this.loadData(params);
                    break;
                case 'status':
                    await this.showStatus(params);
                    break;
                case 'query':
                    await this.queryData(params);
                    break;
                case 'date':
                    await this.handleDateCommand(params);
                    break;
                case 'uddi':
                    await this.handleUddiCommand(params);
                    break;
                case 'help':
                default:
                    this.showHelp();
                    break;
            }
        } catch (error) {
            console.error('❌ 오류 발생:', error.message);
            console.error('📋 자세한 오류:', error.stack);
            process.exit(1);
        }
    }

    showHelp() {
        console.log(`
🦆 UDDI 데이터 수집기

사용법: node scripts/uddi-collector.js <명령어> [옵션]

📋 사용 가능한 명령어:
${Object.entries(this.availableCommands).map(([cmd, desc]) => `  ${cmd.padEnd(10)} - ${desc}`).join('\n')}

📖 명령어 예시:

  # UDDI 목록 보기
  node scripts/uddi-collector.js list

  # 기본 연금 데이터 수집
  node scripts/uddi-collector.js collect pension_workplace

  # 특정 UDDI로 데이터 수집 (강제 업데이트)
  node scripts/uddi-collector.js collect pension_workplace --force

  # 저장된 데이터 로드
  node scripts/uddi-collector.js load pension_workplace

  # 데이터 상태 확인
  node scripts/uddi-collector.js status

  # 사업장별 데이터 조회
  node scripts/uddi-collector.js query pension_workplace --workplace "삼성전자" --start "2024-01" --end "2024-12"

  # 기간별 데이터 조회
  node scripts/uddi-collector.js query pension_workplace --start "2024-01" --end "2024-03"
`);
    }

    async listUddis() {
        console.log('🔍 사용 가능한 UDDI 목록 조회 중...\n');

        try {
            const allUddis = await this.dataCollector.loadDynamicUddis();

            console.log('📋 사용 가능한 UDDI 목록:');
            console.log('=' .repeat(80));

            Object.entries(allUddis).forEach(([name, uddi]) => {
                console.log(`🔹 ${name.padEnd(25)} : ${uddi}`);
            });

            console.log('=' .repeat(80));
            console.log(`📊 총 ${Object.keys(allUddis).length}개의 UDDI 사용 가능\n`);
        } catch (error) {
            console.error('❌ UDDI 목록 로드 실패:', error.message);
        }
    }

    async collectData(params) {
        if (params.length === 0) {
            console.error('❌ UDDI 이름이 필요합니다.');
            console.log('💡 사용법: node scripts/uddi-collector.js collect <uddi_name> [--force]');
            console.log('📋 UDDI 목록 보기: node scripts/uddi-collector.js list');
            return;
        }

        const uddiName = params[0];
        const forceUpdate = params.includes('--force');

        console.log(`🚀 ${uddiName} 데이터 수집 시작...`);
        console.log(`⚙️ 강제 업데이트: ${forceUpdate ? 'ON' : 'OFF'}`);
        console.log();

        try {
            const result = await this.dataCollector.collectAllData(uddiName, forceUpdate);

            if (result.success) {
                console.log('✅ 데이터 수집 완료!');
                console.log(`📊 총 레코드: ${result.totalRecords.toLocaleString()}개`);
                console.log(`📁 저장 위치: ${result.dataFile}`);
                console.log(`⏱️ 소요 시간: ${result.duration}초`);

                if (result.metadata) {
                    console.log(`📈 페이지 수: ${result.metadata.totalPages}`);
                    console.log(`📅 수집 시간: ${new Date(result.metadata.collectedAt).toLocaleString()}`);
                }
            } else {
                console.error('❌ 데이터 수집 실패:', result.error);
            }
        } catch (error) {
            console.error('❌ 데이터 수집 중 오류:', error.message);
        }
    }

    async loadData(params) {
        const uddiName = params.length > 0 ? params[0] : 'pension_workplace';

        console.log(`📂 ${uddiName} 데이터 로드 중...`);

        try {
            const result = await this.dataCollector.loadData(uddiName);

            if (result.success) {
                console.log('✅ 데이터 로드 완료!');
                console.log(`📊 총 레코드: ${result.data.length.toLocaleString()}개`);
                console.log(`📁 파일: ${result.metadata.fileName}`);
                console.log(`📅 수집 시간: ${new Date(result.metadata.collectedAt).toLocaleString()}`);
                console.log(`⏱️ 로드 시간: ${result.metadata.loadTime}초`);

                // 샘플 데이터 표시
                if (result.data.length > 0) {
                    console.log('\n📋 샘플 데이터 (처음 3개):');
                    result.data.slice(0, 3).forEach((item, index) => {
                        console.log(`${index + 1}. ${item['사업장명']} (${item['사업자등록번호'] || 'N/A'})`);
                        console.log(`   - 자료생성년월: ${item['자료생성년월']}`);
                        console.log(`   - 가입자수: ${item['가입자수']}명`);
                    });
                }
            } else {
                console.error('❌ 데이터 로드 실패:', result.error);
            }
        } catch (error) {
            console.error('❌ 데이터 로드 중 오류:', error.message);
        }
    }

    async showStatus(params) {
        console.log('📊 데이터 수집 상태 확인 중...\n');

        try {
            const status = await this.dataCollector.getAvailableData();

            if (status.length === 0) {
                console.log('📭 수집된 데이터가 없습니다.');
                console.log('💡 데이터를 수집하려면: node scripts/uddi-collector.js collect <uddi_name>');
                return;
            }

            console.log('📋 수집된 데이터 현황:');
            console.log('=' .repeat(80));
            console.log('UDDI명'.padEnd(25) + '년월'.padEnd(10) + '레코드수'.padEnd(15) + '수집일시');
            console.log('-' .repeat(80));

            status.forEach(item => {
                const recordCount = item.recordCount.toLocaleString();
                const collectedDate = new Date(item.collectedAt).toLocaleString();
                console.log(`${item.uddiName.padEnd(25)}${item.monthYear.padEnd(10)}${recordCount.padEnd(15)}${collectedDate}`);
            });

            console.log('=' .repeat(80));
            console.log(`📊 총 ${status.length}개의 데이터 파일 보유\n`);

            // UDDI별 통계
            const uddiStats = {};
            status.forEach(item => {
                if (!uddiStats[item.uddiName]) {
                    uddiStats[item.uddiName] = { count: 0, totalRecords: 0 };
                }
                uddiStats[item.uddiName].count++;
                uddiStats[item.uddiName].totalRecords += item.recordCount;
            });

            console.log('📈 UDDI별 통계:');
            Object.entries(uddiStats).forEach(([uddiName, stats]) => {
                console.log(`🔹 ${uddiName}: ${stats.count}개 파일, 총 ${stats.totalRecords.toLocaleString()}개 레코드`);
            });

        } catch (error) {
            console.error('❌ 상태 확인 중 오류:', error.message);
        }
    }

    async queryData(params) {
        if (params.length === 0) {
            console.error('❌ UDDI 이름이 필요합니다.');
            console.log('💡 사용법: node scripts/uddi-collector.js query <uddi_name> --start <YYYY-MM> --end <YYYY-MM> [--workplace <사업장명>]');
            return;
        }

        const uddiName = params[0];

        // 파라미터 파싱
        const startIndex = params.indexOf('--start');
        const endIndex = params.indexOf('--end');
        const workplaceIndex = params.indexOf('--workplace');

        if (startIndex === -1 || endIndex === -1) {
            console.error('❌ 시작 기간(--start)과 종료 기간(--end)이 필요합니다.');
            console.log('💡 사용법: node scripts/uddi-collector.js query <uddi_name> --start <YYYY-MM> --end <YYYY-MM> [--workplace <사업장명>]');
            return;
        }

        const startDate = params[startIndex + 1];
        const endDate = params[endIndex + 1];
        const workplaceName = workplaceIndex !== -1 ? params[workplaceIndex + 1] : null;

        console.log(`🔍 데이터 조회 중...`);
        console.log(`📋 UDDI: ${uddiName}`);
        console.log(`📅 기간: ${startDate} ~ ${endDate}`);
        if (workplaceName) {
            console.log(`🏢 사업장: ${workplaceName}`);
        }
        console.log();

        try {
            const result = await this.dataCollector.queryDataByDateRange(startDate, endDate, uddiName, workplaceName);

            if (result.success) {
                console.log('✅ 데이터 조회 완료!');
                console.log(`📊 총 레코드: ${result.data.length.toLocaleString()}개`);
                console.log(`⏱️ 조회 시간: ${result.metadata?.queryTime || 'N/A'}초`);

                if (result.data.length > 0) {
                    // 사업장별 통계
                    const workplaceStats = {};
                    result.data.forEach(item => {
                        const key = `${item['사업장명']} (${item['사업자등록번호'] || 'N/A'})`;
                        if (!workplaceStats[key]) {
                            workplaceStats[key] = { count: 0, months: new Set() };
                        }
                        workplaceStats[key].count++;
                        workplaceStats[key].months.add(item['자료생성년월']);
                    });

                    console.log('\n📈 사업장별 통계:');
                    const sortedStats = Object.entries(workplaceStats)
                        .sort(([,a], [,b]) => b.count - a.count)
                        .slice(0, 10);

                    sortedStats.forEach(([workplace, stats]) => {
                        console.log(`🔹 ${workplace}: ${stats.count}개 레코드, ${stats.months.size}개월`);
                    });

                    // 샘플 데이터
                    console.log('\n📋 샘플 데이터 (처음 5개):');
                    result.data.slice(0, 5).forEach((item, index) => {
                        console.log(`${index + 1}. ${item['사업장명']} (${item['사업자등록번호'] || 'N/A'})`);
                        console.log(`   - 자료생성년월: ${item['자료생성년월']}`);
                        console.log(`   - 신규취득자수: ${item['신규취득자수']}명, 상실가입자수: ${item['상실가입자수']}명`);
                        console.log(`   - 총 가입자수: ${item['가입자수']}명`);
                    });

                    // CSV 내보내기 옵션
                    if (params.includes('--export')) {
                        const csvFile = path.join(process.cwd(), `${uddiName}_${startDate}_${endDate}.csv`);
                        await this.exportToCsv(result.data, csvFile);
                        console.log(`\n💾 CSV 파일로 내보냄: ${csvFile}`);
                    }
                }
            } else {
                console.error('❌ 데이터 조회 실패:', result.error);
            }
        } catch (error) {
            console.error('❌ 데이터 조회 중 오류:', error.message);
        }
    }

    async exportToCsv(data, filePath) {
        if (data.length === 0) return;

        const headers = Object.keys(data[0]);
        const csvContent = [
            headers.join(','),
            ...data.map(row =>
                headers.map(header => {
                    const value = row[header] || '';
                    // CSV 특수문자 처리
                    if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
                        return `"${value.replace(/"/g, '""')}"`;
                    }
                    return value;
                }).join(',')
            )
        ].join('\n');

        await fs.writeFile(filePath, csvContent, 'utf8');
    }

    // 날짜를 YYYY-MM 형식으로 정규화
    normalizeDate(dateInput) {
        // YYYY-MM 형식 (2024-01)
        if (/^\d{4}-\d{2}$/.test(dateInput)) {
            return dateInput;
        }
        // YYYY/MM 형식 (2024/01)
        if (/^\d{4}\/\d{2}$/.test(dateInput)) {
            return dateInput.replace('/', '-');
        }
        // YYYYMM 형식 (202401)
        if (/^\d{6}$/.test(dateInput)) {
            return `${dateInput.substring(0, 4)}-${dateInput.substring(4, 6)}`;
        }
        // MM/YYYY 형식 (01/2024)
        if (/^\d{2}\/\d{4}$/.test(dateInput)) {
            const [month, year] = dateInput.split('/');
            return `${year}-${month}`;
        }

        return null;
    }

    // 날짜에 해당하는 UDDI 찾기
    async findUddiByDate(targetDate) {
        const normalizedDate = this.normalizeDate(targetDate);
        if (!normalizedDate) {
            throw new Error(`잘못된 날짜 형식입니다. 지원 형식: YYYY-MM, YYYY/MM, YYYYMM, MM/YYYY`);
        }

        console.log(`🔍 ${normalizedDate} 날짜에 해당하는 UDDI 검색 중...`);

        try {
            const allUddis = await this.dataCollector.loadDynamicUddis();

            // 날짜와 매칭되는 UDDI들 찾기
            const matchingUddis = [];

            Object.entries(allUddis).forEach(([name, uddi]) => {
                if (name.includes(normalizedDate.replace('-', '_'))) {
                    matchingUddis.push({ name, uddi });
                }
            });

            if (matchingUddis.length === 0) {
                console.log(`❌ ${normalizedDate} 날짜에 해당하는 UDDI를 찾을 수 없습니다.`);
                console.log(`💡 사용 가능한 날짜를 확인하려면: node scripts/uddi-collector.js list`);
                return null;
            }

            if (matchingUddis.length === 1) {
                console.log(`✅ 발견: ${matchingUddis[0].name}`);
                return matchingUddis[0];
            }

            // 여러 개 발견시 사용자에게 선택하도록 함
            console.log(`🔍 ${normalizedDate}에 해당하는 ${matchingUddis.length}개의 UDDI 발견:`);
            matchingUddis.forEach((uddi, index) => {
                console.log(`  ${index + 1}. ${uddi.name}`);
            });

            // 가장 최근 것 자동 선택
            const selected = matchingUddis[0];
            console.log(`🎯 자동 선택: ${selected.name} (가장 최근)`);
            return selected;

        } catch (error) {
            throw new Error(`UDDI 검색 중 오류: ${error.message}`);
        }
    }

    // 날짜 기반 명령어 처리
    async handleDateCommand(params) {
        if (params.length === 0) {
            console.error('❌ 날짜가 필요합니다.');
            console.log('💡 사용법: node scripts/uddi-collector.js date <YYYY-MM> [collect|query] [--옵션]');
            console.log('📖 예시:');
            console.log('  # 2024년 12월 데이터 수집');
            console.log('  node scripts/uddi-collector.js date 2024-12 collect');
            console.log('  # 2024년 11월 데이터 조회');
            console.log('  node scripts/uddi-collector.js date 2024/11 query --start 2024-01 --end 2024-12');
            console.log('  # 2024년 10월 데이터 로드');
            console.log('  node scripts/uddi-collector.js date 202410 load');
            return;
        }

        const targetDate = params[0];
        const action = params[1] || 'collect'; // 기본 액션은 collect
        const remainingParams = params.slice(2);

        try {
            const uddi = await this.findUddiByDate(targetDate);
            if (!uddi) return;

            console.log(`\n🚀 ${uddi.name}으로 ${action} 실행 중...\n`);

            switch (action) {
                case 'collect':
                    await this.collectData([uddi.name, ...remainingParams]);
                    break;
                case 'load':
                    await this.loadData([uddi.name, ...remainingParams]);
                    break;
                case 'query':
                    await this.queryData([uddi.name, ...remainingParams]);
                    break;
                default:
                    console.error(`❌ 지원하지 않는 액션: ${action}`);
                    console.log('💡 지원 액션: collect, load, query');
                    break;
            }

        } catch (error) {
            console.error('❌ 날짜 기반 명령 실행 중 오류:', error.message);
        }
    }

    // UDDI 식별자 유효성 검증
    isValidUddiId(uddiId) {
        // UUID 형식 검증 (8-4-4-4-12)
        const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        return uuidRegex.test(uddiId);
    }

    // UDDI 식별자와 날짜 기반 명령어 처리
    async handleUddiCommand(params) {
        if (params.length < 2) {
            console.error('❌ UDDI 식별자와 날짜가 모두 필요합니다.');
            console.log('💡 사용법: node scripts/uddi-collector.js uddi <UDDI식별자> <날짜> [collect|load|query] [--옵션]');
            console.log('📖 예시:');
            console.log('  # UDDI로 2024년 12월 데이터 수집');
            console.log('  node scripts/uddi-collector.js uddi 14c0beb5-b153-4b03-892b-8d30a7600de1 2024-12 collect');
            console.log('  # UDDI로 2024년 11월 데이터 로드');
            console.log('  node scripts/uddi-collector.js uddi 14c0beb5-b153-4b03-892b-8d30a7600de1 2024/11 load');
            console.log('  # UDDI로 조회');
            console.log('  node scripts/uddi-collector.js uddi 14c0beb5-b153-4b03-892b-8d30a7600de1 202412 query --start 2024-01 --end 2024-12');
            return;
        }

        const uddiId = params[0];
        const targetDate = params[1];
        const action = params[2] || 'collect'; // 기본 액션은 collect
        const remainingParams = params.slice(3);

        // UDDI 식별자 검증
        if (!this.isValidUddiId(uddiId)) {
            console.error('❌ 잘못된 UDDI 식별자 형식입니다.');
            console.log('💡 UDDI는 UUID 형식이어야 합니다: 예) 14c0beb5-b153-4b03-892b-8d30a7600de1');
            return;
        }

        // 날짜 정규화
        const normalizedDate = this.normalizeDate(targetDate);
        if (!normalizedDate) {
            console.error('❌ 잘못된 날짜 형식입니다.');
            console.log('💡 지원 형식: YYYY-MM, YYYY/MM, YYYYMM, MM/YYYY');
            return;
        }

        console.log(`🔧 UDDI 식별자: ${uddiId}`);
        console.log(`📅 대상 날짜: ${normalizedDate}`);
        console.log(`⚡ 실행 액션: ${action}\n`);

        try {
            // 날짜 기반 UDDI 이름 생성
            const uddiName = `pension_${normalizedDate}`;

            // DataCollector에서 직접 UDDI로 데이터 수집
            console.log(`🚀 ${uddiName}으로 ${action} 실행 중...\n`);

            // 임시로 UDDI 맵핑을 만들어서 사용
            const tempUddiMap = {};
            tempUddiMap[uddiName] = `uddi:${uddiId}`;

            switch (action) {
                case 'collect':
                    await this.collectDataWithUddi(uddiId, uddiName, remainingParams);
                    break;
                case 'load':
                    await this.loadData([uddiName, ...remainingParams]);
                    break;
                case 'query':
                    await this.queryData([uddiName, ...remainingParams]);
                    break;
                default:
                    console.error(`❌ 지원하지 않는 액션: ${action}`);
                    console.log('💡 지원 액션: collect, load, query');
                    break;
            }

        } catch (error) {
            console.error('❌ UDDI 명령 실행 중 오류:', error.message);
        }
    }

    // UDDI 식별자로 직접 데이터 수집
    async collectDataWithUddi(uddiId, uddiName, params) {
        const forceUpdate = params.includes('--force');

        console.log(`🚀 ${uddiName} (${uddiId}) 데이터 수집 시작...`);
        console.log(`⚙️ 강제 업데이트: ${forceUpdate ? 'ON' : 'OFF'}`);
        console.log();

        try {
            // DataCollector의 collectAllData 메서드를 UDDI ID로 직접 호출하도록 수정
            const result = await this.dataCollector.collectDataWithUddiId(uddiId, uddiName, forceUpdate);

            if (result.success) {
                console.log('✅ 데이터 수집 완료!');
                console.log(`📊 총 레코드: ${result.totalRecords.toLocaleString()}개`);
                console.log(`📁 저장 위치: ${result.dataFile}`);
                console.log(`⏱️ 소요 시간: ${result.duration}초`);

                if (result.metadata) {
                    console.log(`📈 페이지 수: ${result.metadata.totalPages}`);
                    console.log(`📅 수집 시간: ${new Date(result.metadata.collectedAt).toLocaleString()}`);
                }
            } else {
                console.error('❌ 데이터 수집 실패:', result.error);
            }
        } catch (error) {
            console.error('❌ 데이터 수집 중 오류:', error.message);
        }
    }
}

// 스크립트 실행
if (require.main === module) {
    const collector = new UDDICollector();
    collector.init().catch(error => {
        console.error('❌ 스크립트 실행 오류:', error);
        process.exit(1);
    });
}

module.exports = UDDICollector;