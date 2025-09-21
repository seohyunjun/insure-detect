require('dotenv').config();
const PensionAPI = require('./src/api/pensionApi');

async function testAPI() {
    console.log('🧪 API 연결 테스트를 시작합니다...\n');

    try {
        const api = new PensionAPI();

        console.log('📝 설정 정보:');
        console.log(`   - API 키: ${process.env.API_KEY ? '설정됨' : '설정되지 않음'}`);
        console.log(`   - Base URL: ${process.env.API_BASE_URL}`);
        console.log('');

        // 1. 기본 API 호출 테스트
        console.log('1️⃣ 기본 API 호출 테스트 (최근 데이터 10개)');
        const basicTest = await api.fetchWorkplaceData({
            numOfRows: 10,
            pageNo: 1
        });

        if (basicTest.success) {
            console.log(`✅ 성공: ${basicTest.data.length}개 데이터 수집`);
            console.log(`   총 데이터 수: ${basicTest.totalCount}개`);

            if (basicTest.data.length > 0) {
                const sample = basicTest.data[0];
                console.log('   샘플 데이터 구조:');
                console.log('   ', JSON.stringify(sample, null, 2));
                console.log('   실제 필드들:', Object.keys(sample));
            }
        } else {
            console.log(`❌ 실패: ${basicTest.error}`);
        }

        console.log('\n2️⃣ 특정 사업장 검색 테스트 (삼성)');
        const searchTest = await api.fetchWorkplaceData({
            bizplcNm: '삼성',
            numOfRows: 5
        });

        if (searchTest.success) {
            console.log(`✅ 성공: ${searchTest.data.length}개 사업장 발견`);
            searchTest.data.forEach((item, index) => {
                console.log(`   ${index + 1}. ${item['사업장명']} (${item['자료생성년월']})`);
            });
        } else {
            console.log(`❌ 실패: ${searchTest.error}`);
        }

        console.log('\n3️⃣ 특정 기간 데이터 테스트 (2025년 7월)');
        const periodTest = await api.fetchWorkplaceData({
            stdrYm: '202507',
            numOfRows: 5
        });

        if (periodTest.success) {
            console.log(`✅ 성공: ${periodTest.data.length}개 데이터 수집`);
            periodTest.data.forEach((item, index) => {
                console.log(`   ${index + 1}. ${item['사업장명']} - 총인원: ${item['가입자수']}명`);
            });
        } else {
            console.log(`❌ 실패: ${periodTest.error}`);
        }

    } catch (error) {
        console.error('💥 테스트 중 오류 발생:', error.message);
    }

    console.log('\n🏁 API 테스트 완료');
}

// 스크립트가 직접 실행될 때만 테스트 실행
if (require.main === module) {
    testAPI();
}

module.exports = testAPI;