#!/usr/bin/env node
/**
 * 모든 사용 가능한 엔드포인트에서 데이터 수집 스크립트
 * 15083277 namespace의 모든 엔드포인트를 동적으로 조회하고 데이터를 수집합니다.
 */

require('dotenv').config();
const DataCollector = require('../src/services/dataCollector');

async function main() {
    console.log('🚀 모든 엔드포인트 데이터 수집 스크립트를 시작합니다...\n');

    const collector = new DataCollector();

    try {
        console.log(`🔑 API 키: ${process.env.API_KEY ? '설정됨' : '설정되지 않음'}`);
        console.log(`🌐 API URL: ${process.env.API_BASE_URL}`);
        console.log('');

        // 모든 사용 가능한 엔드포인트에서 데이터 수집
        const startTime = new Date();
        const results = await collector.collectAllAvailableData();

        const endTime = new Date();
        const duration = Math.round((endTime - startTime) / 1000);

        // 결과 요약
        const successCount = results.filter(r => r.success).length;
        const totalRecords = results.reduce((sum, r) => sum + r.recordCount, 0);

        console.log('\n' + '='.repeat(60));
        console.log('🎉 전체 데이터 수집 완료!');
        console.log('='.repeat(60));
        console.log(`📊 성공한 엔드포인트: ${successCount}/${results.length}개`);
        console.log(`📈 총 레코드 수: ${totalRecords.toLocaleString()}개`);
        console.log(`⏱️ 총 소요 시간: ${duration}초`);
        console.log('='.repeat(60));

        // 실패한 엔드포인트가 있으면 표시
        const failedEndpoints = results.filter(r => !r.success);
        if (failedEndpoints.length > 0) {
            console.log('\n❌ 실패한 엔드포인트:');
            failedEndpoints.forEach(endpoint => {
                console.log(`   - ${endpoint.endpoint}: ${endpoint.error}`);
            });
        }

        // 기존 파일 정리 (30일 이상 된 파일)
        console.log('\n🧹 오래된 파일 정리 중...');
        const deletedCount = await collector.cleanupOldFiles(30);
        console.log(`✅ ${deletedCount}개 파일 정리 완료`);

    } catch (error) {
        console.error('\n❌ 데이터 수집 중 오류 발생:');
        console.error(error.message);
        process.exit(1);
    }
}

// 스크립트가 직접 실행될 때만 실행
if (require.main === module) {
    main();
}

module.exports = main;