# 국민연금 사업장 가입현황 데이터 시각화

UDDI별로 모든 데이터를 로컬에 저장하고 시각화하는 프로젝트입니다.

## 🔄 주요 변경사항 (v3.0)

### 새로운 작동 방식
1. **스마트 데이터 수집**: 모든 가용 엔드포인트를 자동 탐지하여 데이터 수집
2. **Parquet 형식**: 효율적인 저장을 위해 Parquet 형식 사용 (JSON 대비 70% 용량 절약)
3. **API 재시도 로직**: timeout 및 네트워크 오류 시 자동 재시도 (exponential backoff)
4. **중복 방지**: 기존 parquet 파일이 있는 달은 자동으로 스킵
5. **오프라인 작동**: 데이터 수집 후 인터넷 연결 없이도 사용 가능
6. **포괄적 파싱**: 모든 summary 형식을 파싱하여 누락 없는 데이터 수집

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
├── scripts/                    # 데이터 관리 스크립트 (NEW)
│   ├── collect-data.js        # 단일 데이터 수집 스크립트
│   ├── collect-all-endpoints.js # 모든 엔드포인트 자동 수집
│   ├── data-status.js         # 데이터 상태 확인 스크립트
│   └── export-summary-mapping.js # API-Parquet 매핑 CSV 생성
├── temp/                      # 임시 파일 저장소 (NEW)
│   └── summary_parquet_mapping_*.csv # 매핑 정보 CSV
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
# 🚀 권장: 모든 엔드포인트 자동 수집 (15083277 namespace의 모든 데이터)
npm run collect-all

# 또는 특정 데이터만 수집
npm run collect-data

# 데이터 상태 확인
npm run data-status

# API-Parquet 매핑 정보 CSV 생성
npm run export-mapping
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
1. **스마트 엔드포인트 탐지**: OpenAPI 문서를 파싱하여 모든 가용 엔드포인트 자동 발견
2. **포괄적 파싱**: 모든 summary 형식을 파싱 (`국민연금공단_국민연금 가입 사업장 내역_20210217` 등)
3. **배치 데이터 수집**: API를 통해 모든 데이터를 한 번에 수집 (병렬 처리)
4. **Parquet 저장**: 효율적인 Parquet 형식으로 데이터 보관 (JSON 대비 70% 용량 절약)
5. **중복 방지**: 기존 parquet 파일이 있는 달은 자동으로 스킵
6. **재시도 로직**: timeout/네트워크 오류 시 자동 재시도 (exponential backoff)
7. **자동 정리**: 오래된 데이터 파일 자동 정리

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

## 📋 npm 스크립트

```bash
# 🔥 핵심 데이터 관리
npm run collect-all     # 🚀 모든 엔드포인트 자동 수집 (추천)
npm run collect-data    # 특정 데이터 수집
npm run data-status     # 데이터 상태 확인
npm run clean-data      # 데이터 수집 + 상태 확인
npm run export-mapping  # API-Parquet 매핑 CSV 생성

# 애플리케이션 실행
npm run dev            # 개발 모드 실행
npm start              # 프로덕션 모드 실행
npm run test-api       # API 연결 테스트
npm run test-performance # 성능 테스트
```

## 📄 데이터 저장 형식

### Parquet 파일 (기본 형식)
효율적인 저장을 위해 **Parquet 형식**을 사용합니다. JSON 대비 약 70% 용량 절약과 빠른 읽기 성능을 제공합니다.

**저장 예시:**
```
source/data/
├── pension_workplace_2025-07.parquet       # 기본 엔드포인트 (최신)
├── pension_workplace_2025-07_metadata.json # 메타데이터
├── pension_2019-06_2019-05.parquet        # 동적 수집된 데이터 (2019년 6월)
├── pension_2019-05_2019-04.parquet        # 동적 수집된 데이터 (2019년 5월)
├── pension_2018-12_2018-11.parquet        # 동적 수집된 데이터 (2018년 12월)
└── ... (총 100+ 개 파일, 2015-12부터 2025-08까지)
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

## 🎯 스마트 데이터 수집 시스템

### 자동 엔드포인트 탐지
시스템이 자동으로 [OpenAPI 문서](https://infuser.odcloud.kr/oas/docs?namespace=15083277/v1)를 분석하여 모든 가용 엔드포인트를 탐지합니다.

### 포괄적 Summary 파싱
다양한 형식의 summary를 모두 파싱하여 데이터 누락을 방지합니다:

#### 지원하는 Summary 형식
- ✅ `국민연금 가입 사업장 내역 2019년 7월` → `2019-07`
- ✅ `국민연금공단_국민연금 가입 사업장 내역_20210217` → `2021-02`
- ✅ `국민연금공단_국민연금 가입 사업장 내역 2020년 5월_20200520` → `2020-05`
- ✅ `국민연금공단_국민연금 가입 사업장 내역_09/24/2021` → `2021-09`
- ✅ `국민연금공단_국민연금 가입 사업장 내역_10/22/2021` → `2021-10`

### API 안정성 개선
- **재시도 로직**: timeout, DNS 오류, 서버 에러 시 자동 재시도
- **Exponential Backoff**: 재시도 간격을 점진적으로 증가
- **타임아웃 연장**: 30초 → 60초로 증가
- **배치 간 딜레이**: API 부하 방지를 위한 2초 딜레이

### 중복 방지 시스템
기존에 수집된 parquet 파일이 있는 달은 자동으로 스킵하여 불필요한 API 호출을 방지합니다.

```bash
# 실행 예시
⏭️ pension_2019-07 스킵: parquet 파일이 이미 존재함 (2019-07)
📡 pension_2025-08 수집 중...
```

## 📊 매핑 정보 관리

### CSV 매핑 파일 생성
`npm run export-mapping` 명령어로 API 엔드포인트와 parquet 파일 간의 매핑 정보를 CSV로 생성할 수 있습니다.

**생성되는 파일**: `temp/summary_parquet_mapping_[timestamp].csv`

**CSV 구조**:
- `endpoint_path`: API 엔드포인트 경로
- `summary`: OpenAPI summary 원본
- `extracted_year_month`: 추출된 년월 (YYYY-MM)
- `matching_parquet_files`: 매칭되는 parquet 파일들
- `parquet_file_count`: 매칭된 파일 개수
- `status`: 처리 상태 (valid/parse_failed/no_summary)

**통계 예시**:
```
📊 총 엔드포인트: 118개
✅ 유효한 엔드포인트: 118개
❌ 파싱 실패: 0개
📄 parquet 파일 매칭: 77개
```

## ✨ 개선사항

### 장점
1. **🚀 성능 향상**: 로컬 Parquet 데이터 사용으로 빠른 응답 + 70% 용량 절약
2. **🔄 스마트 수집**: 모든 가용 엔드포인트 자동 탐지 및 포괄적 파싱
3. **🛡️ 안정성**: API 재시도 로직으로 네트워크 오류 자동 복구
4. **📋 중복 방지**: 기존 데이터 자동 감지로 불필요한 API 호출 방지
5. **📊 투명성**: 매핑 CSV로 수집 현황 완전 파악
6. **🌐 오프라인 지원**: 데이터 수집 후 인터넷 연결 불필요
7. **💾 데이터 보존**: 수집된 데이터의 영구 보관 및 버전 관리
8. **💰 비용 절약**: 효율적인 API 호출로 사용량 최소화

### 주의사항
- 정기적으로 `npm run collect-all`을 실행하여 최신 데이터 수집
- `source/data/` 폴더의 용량 관리 필요 (100+ 파일)
- API 키는 데이터 수집 시에만 필요

### 사용 팁
```bash
# 💡 처음 사용하는 경우
npm run collect-all      # 모든 데이터 수집
npm run export-mapping   # 수집 현황 확인

# 🔄 정기적 업데이트
npm run collect-all      # 새로운 월 데이터만 자동 수집

# 📊 현황 파악
npm run data-status      # 로컬 데이터 상태
npm run export-mapping   # API-Parquet 매핑 현황
```

## 🔗 관련 링크

- [공공데이터포털](https://www.data.go.kr/)
- [국민연금공단 사업장가입현황정보 API](https://www.data.go.kr/data/15083277/fileData.do)
- [OpenAPI 문서 (15083277/v1)](https://infuser.odcloud.kr/oas/docs?namespace=15083277/v1)
- [Chart.js 문서](https://www.chartjs.org/docs/)
- [Apache Parquet 형식](https://parquet.apache.org/)

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