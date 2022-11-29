# data-platform-api-country-creates-rmq-kube

data-platform-api-country-creates-rmq-kube は、周辺業務システム　を データ連携基盤 と統合することを目的に、API で国データを登録するマイクロサービスです。  
https://xxx.xxx.io/api/API_COUNTRY_SRV/creates/

## 動作環境

data-platform-api-country-creates-rmq-kube の動作環境は、次の通りです。    
・ OS: LinuxOS （必須）    
・ CPU: ARM/AMD/Intel（いずれか必須）    


## 本レポジトリ が 対応する API サービス
data-platform-api-country-creates-rmq-kube が対応する APIサービス は、次のものです。  

APIサービス URL: https://xxx.xxx.io/api/API_COUNTRY_SRV/creates/

## 本レポジトリ に 含まれる API名
data-platform-api-country-creates-rmq-kube には、次の API をコールするためのリソースが含まれています。  

* A_Country（国 - 国データ）
* A_CountryText（国 - 国テキストデータ）

## API への 値入力条件 の 初期値
data-platform-api-country-creates-rmq-kube において、API への値入力条件の初期値は、入力ファイルレイアウトの種別毎に、次の通りとなっています。  

## データ連携基盤のAPIの選択的コール

Latona および AION の データ連携基盤 関連リソースでは、Inputs フォルダ下の sample.json の accepter に取得したいデータの種別（＝APIの種別）を入力し、指定することができます。  
なお、同 accepter にAll(もしくは空白)の値を入力することで、全データ（＝全APIの種別）をまとめて取得することができます。  

* sample.jsonの記載例(1)  

accepter において 下記の例のように、データの種別（＝APIの種別）を指定します。  
ここでは、"Country" が指定されています。    
  
```
	"api_schema": "DPFMCountryCreates",
	"accepter": ["Country"],
	"order_id": null,
	"deleted": false
```
  
* 全データを取得する際のsample.jsonの記載例(2)  

全データを取得する場合、sample.json は以下のように記載します。  

```
	"api_schema": "DPFMCountryCreates",
	"accepter": ["All"],
	"order_id": null,
	"deleted": false
```

## 指定されたデータ種別のコール

accepter における データ種別 の指定に基づいて DPFM_API_Caller 内の caller.go で API がコールされます。  
caller.go の func() 毎 の 以下の箇所が、指定された API をコールするソースコードです。  

```
func (c *DPFMAPICaller) AsyncCountryCreates(
	accepter []string,
	input *dpfm_api_input_reader.SDC,

	log *logger.Logger,
	// msg rabbitmq.RabbitmqMessage,
) []error {
	wg := sync.WaitGroup{}
	mtx := sync.Mutex{}
	errs := make([]error, 0, 5)

	sqlUpdateFin := make(chan error)

	for _, fn := range accepter {
		wg.Add(1)
		switch fn {
		case "Country":
			go c.Country(&wg, &mtx, sqlUpdateFin, log, &errs, input)
		case "CountryText":
			go c.CountryText(&wg, &mtx, sqlUpdateFin, log, &errs, input)
		default:
			wg.Done()
		}
	}

	ticker := time.NewTicker(10 * time.Second)
	select {
	case e := <-sqlUpdateFin:
		if e != nil {
			mtx.Lock()
			errs = append(errs, e)
			return errs
		}
	case <-ticker.C:
		mtx.Lock()
		errs = append(errs, xerrors.New("time out"))
		return errs
	}

	return nil
}
```

## Output  
本マイクロサービスでは、[golang-logging-library-for-data-platform](https://github.com/latonaio/golang-logging-library-for-data-platform) により、以下のようなデータがJSON形式で出力されます。  
以下の sample.json の例は 国 の 国データ が取得された結果の JSON の例です。  
以下の項目のうち、"Country" ～ "CountryName" は、/DPFM_API_Output_Formatter/type.go 内 の Type Country {} による出力結果です。"cursor" ～ "time"は、golang-logging-library による 定型フォーマットの出力結果です。  

```
XXX
```
