#!/usr/bin/env node
/**
 * OpenAPI summary와 parquet 파일 매핑을 CSV로 내보내는 스크립트
 *
 * 이 스크립트는:
 * 1. https://infuser.odcloud.kr/oas/docs?namespace=15083277/v1 에서 API 엔드포인트 정보를 가져옵니다
 * 2. 각 엔드포인트의 summary를 분석하여 년월을 추출합니다
 * 3. 기존 parquet 파일과 매칭합니다
 * 4. 결과를 temp 폴더에 CSV 파일로 저장합니다
 */

require('dotenv').config();
const DataCollector = require('../src/services/dataCollector');

async function main() {
    console.log('📊 OpenAPI summary와 parquet 파일 매핑 CSV 생성 스크립트를 시작합니다...\n');

    const collector = new DataCollector();

    try {
        console.log(`🔑 API 키: ${process.env.API_KEY ? '설정됨' : '설정되지 않음'}`);
        console.log(`🌐 API URL: ${process.env.API_BASE_URL}`);
        console.log('');

        const startTime = new Date();
        const result = await collector.exportSummaryParquetMapping();

        const endTime = new Date();
        const duration = Math.round((endTime - startTime) / 1000);

        if (result.success) {
            console.log('\n' + '='.repeat(60));
            console.log('🎉 CSV 매핑 파일 생성 완료!');
            console.log('='.repeat(60));
            console.log(`📄 CSV 파일 경로: ${result.csvFilePath}`);
            console.log(`📊 총 엔드포인트: ${result.totalEndpoints}개`);
            console.log(`✅ 유효한 엔드포인트: ${result.validEndpoints}개`);
            console.log(`❌ 파싱 실패: ${result.parseFailedCount}개`);
            console.log(`📄 parquet 파일 매칭: ${result.withParquetFiles}개`);
            console.log(`⏱️ 소요 시간: ${duration}초`);
            console.log('='.repeat(60));

            console.log('\n📋 CSV 파일 열 설명:');
            console.log('  - endpoint_path: API 엔드포인트 경로');
            console.log('  - summary: OpenAPI에서 가져온 summary');
            console.log('  - extracted_year_month: 추출된 년월 (YYYY-MM)');
            console.log('  - matching_parquet_files: 매칭되는 parquet 파일들');
            console.log('  - parquet_file_count: 매칭된 파일 개수');
            console.log('  - status: 처리 상태 (valid/parse_failed/no_summary)');
        } else {
            console.error('\n❌ CSV 파일 생성 실패:');
            console.error(result.error);
            process.exit(1);
        }

    } catch (error) {
        console.error('\n❌ 스크립트 실행 중 오류 발생:');
        console.error(error.message);
        process.exit(1);
    }
}

// 스크립트가 직접 실행될 때만 실행
if (require.main === module) {
    main();
}

module.exports = main;