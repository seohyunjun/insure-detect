#!/usr/bin/env node
/**
 * 데이터 상태 확인 스크립트
 * 로컬에 저장된 데이터의 상태를 확인합니다.
 */

require('dotenv').config();
const DataCollector = require('../src/services/dataCollector');
const fs = require('fs').promises;
const path = require('path');

async function main() {
    console.log('📊 데이터 상태 확인을 시작합니다...\n');

    const collector = new DataCollector();

    try {
        // 사용 가능한 데이터 조회
        const availableData = await collector.getAvailableData();

        if (availableData.length === 0) {
            console.log('❌ 저장된 데이터가 없습니다.');
            console.log('💡 다음 명령으로 데이터를 수집하세요: npm run collect-data');
            return;
        }

        console.log('📋 사용 가능한 데이터:');
        console.log('='.repeat(80));

        for (const data of availableData) {
            console.log(`🔹 UDDI: ${data.uddiName}`);
            console.log(`  📅 수집 시간: ${new Date(data.collectedAt).toLocaleString('ko-KR')}`);
            console.log(`  📊 레코드 수: ${data.recordCount.toLocaleString()}개`);
            console.log(`  📁 파일명: ${data.file}`);

            // 파일 크기 확인
            try {
                const filePath = path.join(collector.sourceDir, data.file);
                const stats = await fs.stat(filePath);
                const fileSizeMB = (stats.size / (1024 * 1024)).toFixed(2);
                console.log(`  💾 파일 크기: ${fileSizeMB} MB`);

                // 데이터 샘플 로드
                const result = await collector.loadData(data.uddiName);
                if (result.success && result.data.length > 0) {
                    const sample = result.data[0];
                    console.log(`  🔍 필드 정보: ${Object.keys(sample).length}개 필드`);
                    console.log(`  📝 주요 필드: ${Object.keys(sample).slice(0, 5).join(', ')}...`);

                    // 날짜 범위 확인
                    const dates = result.data
                        .map(item => item['자료생성년월'])
                        .filter(Boolean)
                        .sort();

                    if (dates.length > 0) {
                        const minDate = dates[0];
                        const maxDate = dates[dates.length - 1];
                        console.log(`  📅 데이터 기간: ${minDate} ~ ${maxDate}`);
                    }

                    // 사업장 수 확인
                    const workplaces = new Set(
                        result.data
                            .map(item => item['사업장명'])
                            .filter(Boolean)
                    );
                    console.log(`  🏢 고유 사업장 수: ${workplaces.size.toLocaleString()}개`);
                }
            } catch (error) {
                console.log(`  ⚠️ 파일 정보 읽기 실패: ${error.message}`);
            }

            console.log('');
        }

        console.log('='.repeat(80));
        console.log(`✅ 총 ${availableData.length}개의 데이터셋이 사용 가능합니다.`);

        // 저장소 사용량 확인
        try {
            const sourceDir = collector.sourceDir;
            const files = await fs.readdir(sourceDir);
            const totalSize = await Promise.all(
                files.map(async file => {
                    const filePath = path.join(sourceDir, file);
                    const stats = await fs.stat(filePath);
                    return stats.size;
                })
            );

            const totalSizeMB = totalSize.reduce((a, b) => a + b, 0) / (1024 * 1024);
            console.log(`💾 전체 저장소 사용량: ${totalSizeMB.toFixed(2)} MB`);
        } catch (error) {
            console.log('⚠️ 저장소 사용량 계산 실패');
        }

    } catch (error) {
        console.error('❌ 데이터 상태 확인 중 오류 발생:');
        console.error(error.message);
        process.exit(1);
    }
}

// 스크립트가 직접 실행될 때만 실행
if (require.main === module) {
    main();
}

module.exports = main;