#!/usr/bin/env node
/**
 * 병렬 처리 성능 테스트 스크립트
 * 기간별 데이터 로드 성능을 측정합니다.
 */

require('dotenv').config();
const DataCollector = require('./src/services/dataCollector');

async function testPerformance() {
    console.log('🚀 병렬 처리 성능 테스트를 시작합니다...\n');

    const collector = new DataCollector();

    // 테스트할 기간 설정 (작은 범위로 시작)
    const startDate = '2024-01';
    const endDate = '2024-03';
    const uddiName = 'pension_workplace';

    console.log(`📅 테스트 기간: ${startDate} ~ ${endDate}`);
    console.log(`📋 UDDI: ${uddiName}`);
    console.log(`🔑 API 키: ${process.env.API_KEY ? '설정됨' : '설정되지 않음'}\n`);

    try {
        // 사용 가능한 데이터 파일 확인
        console.log('📁 사용 가능한 데이터 파일 확인 중...');
        const availableData = await collector.getAvailableData();

        if (availableData.length === 0) {
            console.log('⚠️ 테스트할 데이터 파일이 없습니다.');
            console.log('먼저 데이터를 수집해주세요: npm run collect-data');
            return;
        }

        console.log(`📊 발견된 데이터 파일: ${availableData.length}개`);
        availableData.forEach(data => {
            console.log(`  - ${data.uddiName}_${data.monthYear}: ${data.recordCount.toLocaleString()}개 레코드`);
        });

        // 기간 내 파일 찾기
        const testFiles = availableData.filter(data => {
            const monthYear = data.monthYear;
            return monthYear >= startDate && monthYear <= endDate;
        });

        if (testFiles.length === 0) {
            console.log(`⚠️ 테스트 기간 ${startDate} ~ ${endDate} 내의 데이터 파일이 없습니다.`);
            return;
        }

        console.log(`\n🎯 테스트 대상 파일: ${testFiles.length}개`);
        testFiles.forEach(file => {
            console.log(`  - ${file.uddiName}_${file.monthYear}: ${file.recordCount.toLocaleString()}개 레코드`);
        });

        // 병렬 처리 성능 테스트
        console.log('\n🚀 병렬 처리 성능 테스트 시작...');
        const startTime = new Date();

        const result = await collector.loadDataByDateRange(
            startDate,
            endDate,
            uddiName
        );

        const endTime = new Date();
        const duration = (endTime - startTime) / 1000;

        console.log('\n' + '='.repeat(60));
        console.log('🎉 병렬 처리 성능 테스트 완료!');
        console.log('='.repeat(60));

        if (result.success) {
            console.log(`📊 총 레코드 수: ${result.data.length.toLocaleString()}개`);
            console.log(`📁 처리된 파일 수: ${result.filesLoaded}개`);
            console.log(`⏱️ 총 소요 시간: ${duration.toFixed(2)}초`);
            console.log(`🚄 평균 처리 속도: ${Math.round(result.data.length / duration).toLocaleString()}개/초`);
            console.log(`📈 파일당 평균 시간: ${(duration / result.filesLoaded).toFixed(2)}초/파일`);

            // 메모리 사용량 확인
            const memUsage = collector.getMemoryUsage();
            console.log(`💾 메모리 사용량: ${memUsage.usedMB}MB (총 ${memUsage.totalMB}MB)`);

            // 성능 예상치 계산
            const recordsPerSecond = Math.round(result.data.length / duration);
            const estimatedTimeFor12Months = Math.round((result.data.length * 12) / recordsPerSecond);

            console.log(`\n📈 성능 예상치:`);
            console.log(`  - 12개월 데이터 처리 예상 시간: ${estimatedTimeFor12Months}초 (${Math.round(estimatedTimeFor12Months/60)}분)`);
            console.log(`  - 월별 평균 처리 시간: ${(duration / testFiles.length).toFixed(2)}초`);

        } else {
            console.log(`❌ 테스트 실패: ${result.error}`);
        }

        console.log('='.repeat(60));

        // 추가 통계 정보
        console.log('\n📋 병렬 처리 개선 사항:');
        console.log('  ✅ 파일 로딩: 순차 → 병렬 처리');
        console.log('  ✅ API 요청: 페이지별 배치 병렬 처리');
        console.log('  ✅ 메모리 최적화: 가비지 컬렉션 자동 실행');
        console.log('  ✅ 진행상황: 실시간 모니터링');

    } catch (error) {
        console.error('\n❌ 성능 테스트 중 오류 발생:');
        console.error(error.message);
        console.error(error.stack);
        process.exit(1);
    }
}

// 스크립트가 직접 실행될 때만 실행
if (require.main === module) {
    testPerformance();
}

module.exports = testPerformance;