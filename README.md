# 국민연금 사업장 가입현황 데이터 시각화

UDDI별로 모든 데이터를 로컬에 저장하고 시각화하는 프로젝트입니다.

## 🔄 주요 변경사항 (v2.0)

### 새로운 작동 방식
1. **데이터 수집**: API를 통해 모든 데이터를 `source/` 폴더에 저장
2. **로컬 처리**: 저장된 데이터를 사용하여 시각화 및 분석
3. **오프라인 작동**: 데이터 수집 후 인터넷 연결 없이도 사용 가능

## 📊 프로젝트 개요

이 프로젝트는 공공데이터포털(data.go.kr)에서 제공하는 국민연금 사업장 가입현황 API를 활용하여:
- 사업장별 월별 신규 가입자 수
- 사업장별 월별 상실 가입자 수
- 사업장별 월별 총 가입자 수
- 위 데이터들의 시간별 변화 추이를 차트로 시각화

## 🏗️ 프로젝트 구조

```
data/
├── source/              # 데이터 저장소 (NEW)
│   ├── data/           # 수집된 데이터 파일 (JSON)
│   └── logs/           # 수집 로그 파일
├── scripts/             # 데이터 관리 스크립트 (NEW)
│   ├── collect-data.js # 데이터 수집 스크립트
│   └── data-status.js  # 데이터 상태 확인 스크립트
├── src/
│   ├── index.js              # 메인 애플리케이션
│   ├── services/             # 서비스 레이어 (NEW)
│   │   └── dataCollector.js  # 데이터 수집 서비스
│   ├── api/
│   │   └── pensionApi.js     # 로컬 데이터 API (수정됨)
│   └── data/
│       └── processor.js      # 데이터 처리 및 변환 로직
├── public/
│   ├── css/, js/, index.html # 프론트엔드 파일들
├── .env                      # 환경변수 (API 키 등)
├── .env.example              # 환경변수 예시 파일
├── package.json              # 프로젝트 설정 및 의존성
└── README.md                 # 프로젝트 문서
```

## 🚀 설치 및 실행 단계

### 1단계: 프로젝트 설정

```bash
# 의존성 패키지 설치
npm install
```

### 2단계: API 키 설정

1. [공공데이터포털](https://www.data.go.kr/)에 회원가입
2. [국민연금공단_사업장가입현황정보](https://www.data.go.kr/data/15083277/fileData.do) 데이터 활용신청
3. 발급받은 API 키를 `.env` 파일에 설정:

```bash
# .env.example을 복사하여 .env 파일 생성
cp .env.example .env

# .env 파일을 열어 API 키 입력
# API_KEY=your_api_key_here
```

### 3단계: 데이터 수집

**중요**: 애플리케이션 사용 전에 먼저 데이터를 수집해야 합니다:

```bash
# 모든 데이터 수집
npm run collect-data

# 데이터 상태 확인
npm run data-status
```

### 4단계: 애플리케이션 실행

```bash
# 개발 모드로 실행
npm run dev

# 또는 일반 실행
npm start
```

### 5단계: 브라우저에서 확인

브라우저에서 `http://localhost:3000`에 접속하여 시각화 결과를 확인합니다.

## 📈 데이터 구조

API에서 제공하는 주요 데이터 필드:
- `stdrYm`: 기준년월
- `bizplcNm`: 사업장명
- `newAcqsCnt`: 신규취득자수 (입사자)
- `lossCnt`: 상실자수 (퇴사자)
- `tkcgCnt`: 취득자수 (총 인원)

## 🔧 주요 기능

### 데이터 관리
1. **배치 데이터 수집**: API를 통해 모든 데이터를 한 번에 수집
2. **로컬 저장소**: JSON 형태로 안전하게 데이터 보관
3. **데이터 버전 관리**: 타임스탬프가 포함된 파일명으로 버전 관리
4. **자동 정리**: 오래된 데이터 파일 자동 정리

### 시각화 및 분석
1. **고속 처리**: 로컬 데이터 사용으로 빠른 응답 속도
2. **오프라인 지원**: 데이터 수집 후 인터넷 연결 없이 사용 가능
3. **시각화**: Chart.js를 활용한 인터랙티브 차트 생성
4. **필터링**: 사업장명으로 특정 회사 데이터 조회
5. **시계열 분석**: 월별 변화 추이 분석

### 새로운 API 엔드포인트
- `POST /api/collect-data` - 새로운 데이터 수집
- `GET /api/available-data` - 사용 가능한 데이터 조회

## 🛠️ 사용 기술

- **Backend**: Node.js, Express.js
- **Frontend**: HTML5, CSS3, JavaScript
- **시각화**: Chart.js (클라이언트 사이드)
- **API 통신**: Axios
- **데이터 처리**: Moment.js
- **환경 관리**: dotenv

> **참고**: 이전 버전에서 서버 사이드 차트 생성을 위해 `chartjs-node-canvas`를 사용했으나, macOS 호환성 문제로 인해 클라이언트 사이드 차트 생성으로 변경되었습니다.

## 📋 새로운 npm 스크립트

```bash
# 데이터 관리
npm run collect-data    # 모든 데이터 수집
npm run data-status     # 데이터 상태 확인
npm run clean-data      # 데이터 수집 + 상태 확인

# 기존 스크립트
npm run dev            # 개발 모드 실행
npm start              # 프로덕션 모드 실행
npm run test-api       # API 연결 테스트
```

## 📄 데이터 저장 형식

### Parquet 파일 (기본 형식)
효율적인 저장을 위해 **Parquet 형식**을 사용합니다. JSON 대비 약 70% 용량 절약과 빠른 읽기 성능을 제공합니다.

**저장 예시:**
```
source/data/
├── pension_workplace_2024-05.parquet      # 실제 데이터 (Parquet 형식)
├── pension_workplace_2024-05_metadata.json # 메타데이터 (JSON 형식)
└── pension_workplace_2024-06.parquet      # 다음 월 데이터
```

### 메타데이터 파일 형식
```json
{
  "uddi": "uddi:20ddf65d-51d8-421f-8ee5-b64f05554151",
  "uddiName": "pension_workplace",
  "collectedAt": "2025-01-21T10:30:00Z",
  "totalRecords": 50000,
  "totalPages": 50,
  "version": "1.0",
  "columnTypes": {
    "가입자수": "INTEGER",
    "사업장명": "VARCHAR(100)",
    "자료생성년월": "VARCHAR(6)",
    "신규취득자수": "INTEGER",
    "상실가입자수": "INTEGER",
    "사업자등록번호": "VARCHAR(10)"
  },
  "schema": {
    "가입자수": { "type": "INT32" },
    "사업장명": { "type": "UTF8" },
    "자료생성년월": { "type": "UTF8" },
    "신규취득자수": { "type": "INT32" },
    "상실가입자수": { "type": "INT32" }
  }
}
```

### 정리된 데이터 구조
원본 API에서 제공하는 복잡한 컬럼명을 정리하여 저장합니다:

**원본:** `가입자수 JNNGP_CNT INTEGER`
**정리후:** `가입자수`

**데이터 예시:**
```json
{
  "가입자수": 15,
  "고객법정동주소코드": "4127310500",
  "고객행정동주소코드": "4127357000",
  "당월고지금액": 4022200,
  "사업장명": "(주)신성일렉스",
  "사업장업종코드": "319001",
  "사업장업종코드명": "운송장비용 조명장치 제조업",
  "자료생성년월": "2016-05",
  "신규취득자수": 0,
  "상실가입자수": 0
}
```

### 호환성
기존 JSON 파일도 계속 지원하므로 점진적 마이그레이션이 가능합니다.

## ✨ 개선사항

### 장점
1. **성능 향상**: 로컬 데이터 사용으로 빠른 응답
2. **안정성**: API 호출 제한 없음
3. **오프라인 지원**: 데이터 수집 후 인터넷 연결 불필요
4. **데이터 보존**: 수집된 데이터의 버전 관리
5. **비용 절약**: API 호출 횟수 최소화

### 주의사항
- 정기적으로 `npm run collect-data`를 실행하여 최신 데이터 수집
- `source/data/` 폴더의 용량 관리 필요
- API 키는 데이터 수집 시에만 필요

## 🔗 관련 링크

- [공공데이터포털](https://www.data.go.kr/)
- [국민연금공단 사업장가입현황정보 API](https://www.data.go.kr/data/15083277/fileData.do)
- [Chart.js 문서](https://www.chartjs.org/docs/)

## 📄 라이센스

MIT License

## 🤝 기여하기

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

⚠️ **주의사항**:
- API 키는 절대 공개 저장소에 업로드하지 마세요
- 공공데이터 이용약관을 준수해주세요
- API 호출 제한량을 확인하고 적절히 사용해주세요