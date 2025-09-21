#!/usr/bin/env node
/**
 * 데이터 수집 스크립트
 * UDDI별로 모든 데이터를 수집하여 로컬에 저장합니다.
 */

require('dotenv').config();
const DataCollector = require('../src/services/dataCollector');

async function main() {
    console.log('🚀 데이터 수집 스크립트를 시작합니다...\n');

    // 도움말 표시
    const args = process.argv.slice(2);
    if (args.includes('--help') || args.includes('-h')) {
        console.log('사용법:');
        console.log('  npm run collect-data [uddiName] [--force]');
        console.log('');
        console.log('옵션:');
        console.log('  --force, -f    기존 데이터가 있어도 강제로 새로 수집');
        console.log('  --help, -h     이 도움말 표시');
        console.log('');
        console.log('예시:');
        console.log('  npm run collect-data                    # 기본 수집 (캐시 사용)');
        console.log('  npm run collect-data-force              # 강제 업데이트');
        console.log('  npm run collect-data pension_workplace  # 특정 UDDI 수집');
        return;
    }

    const collector = new DataCollector();

    try {
        // 명령행 인수 처리
        const args = process.argv.slice(2);
        const uddiName = args[0] || 'pension_workplace';
        const forceUpdate = args.includes('--force') || args.includes('-f');

        console.log(`📋 수집 대상: ${uddiName}`);
        console.log(`🔑 API 키: ${process.env.API_KEY ? '설정됨' : '설정되지 않음'}`);
        console.log(`🌐 API URL: ${process.env.API_BASE_URL}`);
        console.log(`🔄 강제 업데이트: ${forceUpdate ? '예' : '아니오'}`);
        console.log('');

        // 데이터 수집 시작
        const startTime = new Date();
        const result = await collector.collectAllData(uddiName, forceUpdate);

        const endTime = new Date();
        const duration = Math.round((endTime - startTime) / 1000);

        console.log('\n' + '='.repeat(50));
        console.log(result.fromCache ? '🎉 기존 데이터 사용 완료!' : '🎉 데이터 수집 완료!');
        console.log('='.repeat(50));
        console.log(`📊 총 레코드 수: ${result.recordCount.toLocaleString()}개`);
        console.log(`⏱️ 소요 시간: ${duration}초`);
        console.log(`💾 데이터 파일: ${result.dataFile}`);
        console.log(`🔗 최신 파일: ${result.latestFile}`);
        console.log(`📅 수집 시간: ${result.metadata.collectedAt}`);
        console.log(`💿 캐시 사용: ${result.fromCache ? '예' : '아니오'}`);
        console.log('='.repeat(50));

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