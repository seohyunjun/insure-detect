#!/usr/bin/env node
/**
 * 사용 가능한 엔드포인트 목록 조회 스크립트
 * OpenAPI 문서에서 동적으로 엔드포인트를 로드하고 목록을 표시합니다.
 */

require('dotenv').config();
const DataCollector = require('../src/services/dataCollector');

async function main() {
    console.log('🔍 사용 가능한 엔드포인트 목록을 조회합니다...\n');

    const collector = new DataCollector();

    try {
        // 동적 UDDI 로딩
        const allUddis = await collector.loadDynamicUddis();

        console.log('📋 사용 가능한 엔드포인트 목록:');
        console.log('='.repeat(60));

        const sortedUddis = Object.entries(allUddis).sort((a, b) => a[0].localeCompare(b[0]));

        sortedUddis.forEach(([name, path], index) => {
            const isStatic = collector.uddis[name] ? '🔧' : '🔄';
            console.log(`${(index + 1).toString().padStart(2, ' ')}. ${isStatic} ${name}`);
            console.log(`    ${path}`);
            console.log('');
        });

        console.log('='.repeat(60));
        console.log(`총 ${sortedUddis.length}개 엔드포인트`);
        console.log('');
        console.log('범례:');
        console.log('🔧 정적 엔드포인트 (기본 설정)');
        console.log('🔄 동적 엔드포인트 (OpenAPI 문서에서 로드)');
        console.log('');
        console.log('사용법:');
        console.log('  npm run collect-data [엔드포인트명]');
        console.log('');
        console.log('예시:');
        sortedUddis.slice(0, 3).forEach(([name]) => {
            console.log(`  npm run collect-data ${name}`);
        });

    } catch (error) {
        console.error('❌ 엔드포인트 목록 조회 중 오류 발생:');
        console.error(error.message);
        process.exit(1);
    }
}

// 스크립트가 직접 실행될 때만 실행
if (require.main === module) {
    main();
}

module.exports = main;