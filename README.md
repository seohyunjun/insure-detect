# 국민연금 사업장 가입현황 시각화 프로젝트

한국 국민연금공단의 사업장별 가입현황 데이터를 활용하여 사업장명별 시간에 따른 퇴사자/입사자/총 인원 변화를 시각화하는 프로젝트입니다.

## 📊 프로젝트 개요

이 프로젝트는 공공데이터포털(data.go.kr)에서 제공하는 국민연금 사업장 가입현황 API를 활용하여:
- 사업장별 월별 신규 가입자 수
- 사업장별 월별 상실 가입자 수
- 사업장별 월별 총 가입자 수
- 위 데이터들의 시간별 변화 추이를 차트로 시각화

## 🏗️ 프로젝트 구조

```
pension-workplace-visualization/
├── src/
│   ├── index.js              # 메인 애플리케이션
│   ├── api/
│   │   └── pensionApi.js     # 국민연금 API 연동 모듈
│   ├── data/
│   │   └── processor.js      # 데이터 처리 및 변환 로직
│   └── visualization/
│       └── chartGenerator.js # 차트 생성 컴포넌트
├── public/
│   ├── css/
│   │   └── style.css         # 스타일시트
│   ├── js/
│   │   └── main.js           # 프론트엔드 스크립트
│   └── index.html            # 메인 HTML 페이지
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

### 3단계: 애플리케이션 실행

```bash
# 개발 모드로 실행
npm run dev

# 또는 일반 실행
npm start
```

### 4단계: 브라우저에서 확인

브라우저에서 `http://localhost:3000`에 접속하여 시각화 결과를 확인합니다.

## 📈 데이터 구조

API에서 제공하는 주요 데이터 필드:
- `stdrYm`: 기준년월
- `bizplcNm`: 사업장명
- `newAcqsCnt`: 신규취득자수 (입사자)
- `lossCnt`: 상실자수 (퇴사자)
- `tkcgCnt`: 취득자수 (총 인원)

## 🔧 주요 기능

1. **데이터 수집**: 공공데이터 API를 통한 실시간 데이터 수집
2. **데이터 처리**: 사업장별, 월별 데이터 가공 및 정리
3. **시각화**: Chart.js를 활용한 인터랙티브 차트 생성
4. **필터링**: 사업장명으로 특정 회사 데이터 조회
5. **시계열 분석**: 월별 변화 추이 분석

## 🛠️ 사용 기술

- **Backend**: Node.js, Express.js
- **Frontend**: HTML5, CSS3, JavaScript
- **시각화**: Chart.js
- **API 통신**: Axios
- **데이터 처리**: CSV Parser, Moment.js
- **환경 관리**: dotenv

## 📝 개발 가이드

### API 호출 예시
```javascript
// 특정 기간의 사업장 데이터 조회
const data = await fetchPensionData({
  startDate: '202301',
  endDate: '202312',
  workplace: '삼성전자'
});
```

### 차트 생성 예시
```javascript
// 시계열 차트 생성
const chart = generateTimeSeriesChart({
  labels: ['2023-01', '2023-02', '2023-03'],
  datasets: [{
    label: '신규입사자',
    data: [120, 150, 180]
  }]
});
```

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