# BÁO CÁO KẾT QUẢ NOTEBOOK 03
## Gold Training Pairs – Chuẩn bị dữ liệu huấn luyện cho HybriNews

**Dự án:** HybriNews – Hệ thống tóm tắt tin tức đa ngôn ngữ  
**Giai đoạn:** Gold Data Preparation & Quality Gate  
**Công nghệ:** Python 3.13, PySpark DataFrame, Spark SQL, Parquet  
**Môi trường thực hiện:** Windows Local; notebook cấu hình Spark `local[2]`, 8 shuffle partitions và driver memory `4g`  
**Ngày ghi nhận Gold commit:** 10/11/2026, 09:25:16 UTC  
**Dữ liệu:** CNN/DailyMail, XSum, VietNews  
**Bằng chứng:** `precheck_manifest.json`, `quality_summary.csv`, `length_statistics.csv`, `gold_summary.csv`, `gold_commit_manifest.json` và mã nguồn Notebook 03.

---

## 1. Mục tiêu

Notebook 03 chuyển dữ liệu đã qua Silver Cleaning thành **Gold Training Pairs**, tức các cặp văn bản nguồn – bản tóm tắt tham chiếu có nhãn nhiệm vụ rõ ràng, sẵn sàng cho các bước xử lý chuyên biệt trước huấn luyện.

Các mục tiêu cụ thể là: kiểm tra tính toàn vẹn của Silver đã chốt; nhận diện bản ghi có dấu hiệu bất thường; yêu cầu quyết định KEEP/DROP và xác nhận phân công nhiệm vụ; tạo các trường dữ liệu chuẩn cho mô hình; bảo toàn train/validation/test; cuối cùng ghi Gold theo cơ chế kiểm tra trước khi công bố. Notebook này **không huấn luyện mô hình, không tạo reference mới, không tokenization và không truncation**.

## 2. Dữ liệu đầu vào và phạm vi nhiệm vụ

Ba bộ dữ liệu được giữ lại từ giai đoạn lựa chọn dataset:

| Dataset | Ngôn ngữ | Nhiệm vụ | Số bản ghi Silver |
|---|---|---|---:|
| CNN/DailyMail | Tiếng Anh (`en`) | Normal Summarization | 308.449 |
| XSum | Tiếng Anh (`en`) | Super Summarization | 226.413 |
| VietNews | Tiếng Việt (`vi`) | Super Summarization | 143.785 |
| **Tổng cộng** | **2 ngôn ngữ** | **3 cấu hình dataset–task** | **678.647** |

XL-Sum Vietnamese không tham gia Notebook 03 vì đã được loại khỏi phạm vi huấn luyện chính thức ở giai đoạn lựa chọn dữ liệu. Quyết định này nhằm giảm chi phí thực nghiệm, không phải kết luận XL-Sum VI có chất lượng thấp.

Notebook sử dụng `silver_commit_manifest.json` để đối chiếu số dòng và các split trước khi xử lý. Dữ liệu Silver được đọc dưới dạng Parquet bằng PySpark DataFrame.

## 3. Phương pháp thực hiện

### 3.1. Giai đoạn A — Precheck và xuất phiếu đánh giá

Notebook kiểm tra các cột Silver bắt buộc: `dataset`, `language`, `split`, `article_id`, `record_id`, `source_text`, `target_text`, `source_hash`, `target_hash`. Sau đó kiểm tra tính đầy đủ của văn bản, mã hash/ID, đúng nhãn dataset và ngôn ngữ, số dòng từng split theo Silver manifest và tình trạng trùng `source_hash` trong mỗi dataset.

Các bản ghi được **gắn cờ cần xem xét**, không tự động bị loại, theo các điều kiện:

| Cờ kiểm tra | Điều kiện trong notebook | Ý nghĩa |
|---|---|---|
| `VERY_SHORT_SOURCE` | Source dưới 20 đơn vị tách bằng khoảng trắng | Nghi thiếu nội dung bài báo |
| `VERY_SHORT_TARGET` | Target dưới 4 đơn vị | Nghi bản tóm tắt không đủ nội dung |
| `TARGET_LONGER_THAN_SOURCE` | Số đơn vị target lớn hơn source | Cặp source–target cần kiểm tra |
| `REPLACEMENT_CHAR_SOURCE/TARGET` | Có ký tự Unicode thay thế `�` | Có khả năng gặp vấn đề mã hóa |
| `VERY_LONG_SOURCE` | Source dài trên 60.000 ký tự | Nghi nội dung bất thường hoặc quá dài |
| `VERY_LONG_TARGET` | Target dài trên 1.500 ký tự | Cần xem xét độ dài reference |

Các ngưỡng trên là **quy tắc sàng lọc được nhóm lựa chọn**, không phải bằng chứng tự thân rằng một bản ghi chắc chắn bị lỗi. Số đơn vị tách bằng khoảng trắng cũng không tương đương token của tokenizer mô hình.

Giai đoạn A xuất `quality_summary.csv`, `length_statistics.csv`, ba file `*_risk_review.csv`, `task_approval.csv` và `precheck_manifest.json`. Manifest ở trạng thái `REVIEW_PENDING`, vì vậy tại thời điểm này **chưa ghi Gold**.

### 3.2. Giai đoạn B — Áp dụng phê duyệt và tạo Gold

Sau khi xem xét kết quả, nhóm quyết định **DROP toàn bộ bản ghi bị gắn cờ**, tương ứng 495 dòng. Đây là chính sách lọc thận trọng để giảm rủi ro đưa nguồn thiếu nội dung, target không phù hợp hoặc văn bản có dấu hiệu lỗi vào dữ liệu huấn luyện. Không mô tả việc này là 495 lỗi đã được con người xác minh độc lập.

Notebook yêu cầu phê duyệt chính xác ba cặp dataset–task trong `task_approval.csv`; mọi dòng risk review phải có quyết định `KEEP` hoặc `DROP`. Trước khi ghi, chương trình so khớp danh sách `record_id` với precheck manifest, kiểm tra checksum các phiếu và kiểm tra dữ liệu Silver không thay đổi kể từ giai đoạn A.

Các bản ghi được chọn loại được thực hiện bằng phép `left_anti join` theo `record_id`. Notebook tạo Gold **tạm**, đọc lại Parquet để kiểm tra số dòng, nhãn nhiệm vụ, nội dung không rỗng và source hash duy nhất trong mỗi dataset; cuối cùng mới chuyển ba thư mục sang `data/gold/` và tạo Gold commit manifest ở trạng thái `COMPLETE`. Quy trình không tự ghi đè Gold cũ.

## 4. Kết quả precheck và các trường hợp bất thường

### 4.1. Tổng quan

| Dataset | Silver kiểm tra | Bản ghi gắn cờ | Tỷ lệ trên Silver |
|---|---:|---:|---:|
| CNN/DailyMail | 308.449 | 120 | 0,0389% |
| XSum | 226.413 | 371 | 0,1639% |
| VietNews | 143.785 | 4 | 0,0028% |
| **Tổng** | **678.647** | **495** | **0,0729%** |

Có **495 bản ghi** mang ít nhất một dấu hiệu cần rà soát. Một bản ghi có thể mang nhiều cờ, vì vậy tổng số lần xuất hiện của các loại cờ có thể lớn hơn số bản ghi.

### 4.2. CNN/DailyMail

Trong 120 bản ghi gắn cờ, hệ thống ghi nhận: 89 lần `VERY_LONG_TARGET`, 31 lần `TARGET_LONGER_THAN_SOURCE`, 21 lần `REPLACEMENT_CHAR_SOURCE` và 2 lần `VERY_SHORT_SOURCE`.

CNN/DailyMail phục vụ Normal Summarization, nên một reference dài chưa đương nhiên là lỗi. Tuy vậy, để áp dụng một chính sách sàng lọc thống nhất, nhóm quyết định loại cả 120 bản ghi có cờ. Đây là lựa chọn thận trọng nhằm ưu tiên tính nhất quán của tập huấn luyện; nó có thể làm mất một số mẫu hợp lệ và cần được công bố như một hạn chế.

Theo phiếu precheck, các bản ghi có cờ được phân bố: 98 train, 10 validation, 12 test.

### 4.3. XSum

XSum có 371 bản ghi được gắn cờ. Các cờ gồm: 203 lần `TARGET_LONGER_THAN_SOURCE`, 174 lần `VERY_SHORT_SOURCE`, 134 lần `VERY_SHORT_TARGET` và 6 lần `VERY_LONG_SOURCE`.

XSum có nhiều trường hợp nguồn rất ngắn hoặc reference không đủ thông tin. Các biểu hiện như văn bản điều hướng trang web, thông báo nội dung sẽ được cập nhật hay target không tạo thành tóm tắt có nghĩa là rủi ro đối với cặp huấn luyện source–target. Nhóm quyết định loại toàn bộ 371 bản ghi gắn cờ theo chính sách đã chốt.

Theo phiếu precheck, các bản ghi có cờ được phân bố: 332 train, 17 validation, 22 test. Đây cũng là dataset có tỷ lệ loại thêm cao nhất ở Notebook 03, khoảng 0,1639% Silver.

### 4.4. VietNews

VietNews chỉ có 4 bản ghi gắn cờ: 3 lần `REPLACEMENT_CHAR_SOURCE` và 1 lần `TARGET_LONGER_THAN_SOURCE`, phân bố 3 train và 1 test.

Tỷ lệ gắn cờ rất thấp so với quy mô 143.785 bản ghi Silver. Nhóm quyết định loại cả bốn dòng để tuân thủ quy tắc đã lựa chọn. Những dấu `_` dùng trong word segmentation **không được coi là lỗi** và không thuộc tiêu chí loại dữ liệu.

## 5. Kết quả tạo Gold chính thức

### 5.1. Số lượng trước và sau xử lý

| Dataset | Silver | DROP tại Notebook 03 | Gold cuối cùng | Tỷ lệ giữ từ Silver |
|---|---:|---:|---:|---:|
| CNN/DailyMail | 308.449 | 120 | 308.329 | 99,9611% |
| XSum | 226.413 | 371 | 226.042 | 99,8361% |
| VietNews | 143.785 | 4 | 143.781 | 99,9972% |
| **Tổng** | **678.647** | **495** | **678.152** | **99,9271%** |

Kiểm tra cân đối: **678.647 − 495 = 678.152**. Các kết quả `gold_summary.csv` khớp số dòng và phân phối split trong `gold_commit_manifest.json`.

So với Bronze đã lựa chọn ở đầu pipeline (682.498 dòng), Gold còn 678.152 dòng: giảm tổng cộng **4.346 bản ghi** qua Silver và Gold, tương đương giữ lại khoảng **99,36%** dữ liệu đầu vào. Con số này không bao gồm XL-Sum VI vì dataset đó không thuộc phạm vi chính thức.

### 5.2. Phân phối theo tập dữ liệu

| Dataset | Train | Validation | Test | Tổng Gold |
|---|---:|---:|---:|---:|
| CNN/DailyMail | 283.513 | 13.351 | 11.465 | 308.329 |
| XSum | 203.434 | 11.304 | 11.304 | 226.042 |
| VietNews | 99.102 | 22.182 | 22.497 | 143.781 |
| **Tổng cộng** | **586.049** | **46.837** | **45.266** | **678.152** |

Như vậy, tập Gold có **586.049 bản ghi train (86,42%)**, **46.837 validation (6,91%)** và **45.266 test (6,67%)**. Các split gốc được bảo toàn về tên và mục đích; Notebook 03 chỉ loại các dòng được chọn, không chia ngẫu nhiên lại dữ liệu.

Số dòng bị loại theo split: **433 train, 27 validation và 35 test**. Các bản ghi trong tập test chỉ bị lọc theo chính sách chất lượng đã xác định; không được dùng điểm mô hình trên test để lựa chọn phương pháp hoặc hyperparameter.

### 5.3. Đối chiếu ba nhiệm vụ

| Dataset | `language` | `task` | Vai trò trong thí nghiệm |
|---|---|---|---|
| CNN/DailyMail | `en` | `normal` | Reference tiếng Anh thiên về nhiều câu, nhiều ý |
| XSum | `en` | `super` | Reference tiếng Anh ngắn, tập trung thông điệp chính |
| VietNews | `vi` | `super` | Reference tiếng Việt ngắn, phục vụ siêu tóm tắt |

Notebook chỉ **gắn nhãn nhiệm vụ cho reference có sẵn**, không tự sinh tóm tắt mới, không ép VietNews thành Normal và không khẳng định đã đánh giá xong factuality của toàn bộ corpus.

## 6. Cấu trúc dữ liệu Gold

Các cặp Gold lưu lại thông tin phục vụ truy vết và huấn luyện, gồm:

- **Thông tin nguồn gốc:** `dataset`, `language`, `split`, `article_id`, `record_id`, `source_hash`, `target_hash`.
- **Nội dung:** `source_text`, `target_text`, `model_source_text`, `model_target_text`.
- **Thông tin nhiệm vụ:** `task`, `sample_id`, `gold_schema_version` (giá trị `1.0`).

`sample_id` được tính từ dataset, nhãn nhiệm vụ và `record_id` bằng SHA-256, giúp định danh cặp huấn luyện một cách có thể tái lập.

Riêng VietNews, hai cột `source_text` và `target_text` **giữ dấu `_`** của biểu diễn word segmentation. Các cột `model_source_text` và `model_target_text` chuyển `_` thành khoảng trắng để cung cấp thêm biểu diễn tiếng Việt tự nhiên cho bước xây dựng đầu vào mô hình. Việc chọn biểu diễn nào tốt nhất còn cần kiểm tra với tokenizer và thực nghiệm.

Gold được ghi dưới dạng **Parquet**, phân vùng theo `split`, với thư mục riêng cho `cnn_dailymail`, `xsum` và `vietnews`.

## 7. Kiểm tra chất lượng và khả năng tái lập

Theo mã nguồn Notebook 03, trước khi công bố Gold, chương trình thực hiện những kiểm tra sau:

1. Đối chiếu số dòng và số dòng từng split với Silver commit manifest.
2. Kiểm tra schema, source/target không rỗng và các trường ID/hash quan trọng.
3. Kiểm tra không còn nhiều bản ghi có cùng `source_hash` **trong từng dataset**.
4. Yêu cầu duyệt đầy đủ các phiếu review và đúng bộ ba nhãn nhiệm vụ.
5. Đối chiếu danh sách bản ghi cần xem với `precheck_manifest.json`.
6. Kiểm tra checksum các file phê duyệt và manifest, đồng thời kiểm tra snapshot kích thước/thời gian sửa đổi của Parquet Silver trước khi ghi.
7. Ghi Gold tạm, đọc lại, kiểm tra cân đối `Silver − DROP = Gold`, các split và nhãn nhiệm vụ.
8. Chỉ chuyển sang thư mục Gold chính thức khi đạt điều kiện kiểm tra; không ghi đè Gold cũ.

`gold_commit_manifest.json` ghi trạng thái **`COMPLETE`**, thời điểm commit, đường dẫn đầu ra, số dòng từng dataset/split và SHA-256 của manifest Silver, manifest precheck cùng các file review. Đây là bằng chứng về quy trình xử lý có kiểm soát; snapshot dựa trên kích thước/thời gian sửa file **không phải checksum nội dung đầy đủ của từng Parquet**.

**Giới hạn xác minh của báo cáo:** Các số liệu trong phần kết quả đã được đối chiếu giữa CSV/JSON và mã nguồn notebook; người viết báo cáo chưa trực tiếp đọc lại các tệp Gold Parquet trên máy thực thi. Vì vậy, trạng thái COMPLETE là kết quả được notebook ghi nhận, không được trình bày như một cuộc kiểm định độc lập trên toàn bộ byte dữ liệu.

## 8. Hạn chế của kết quả

Thứ nhất, 495 bản ghi bị loại theo chính sách dựa trên cờ cảnh báo. Một cờ về độ dài bất thường hoặc ký tự thay thế không chứng minh tuyệt đối bản ghi đó sai; chính sách lọc có thể loại cả dữ liệu hợp lệ. Những ngưỡng này chưa được tối ưu thông qua phép thử độ nhạy.

Thứ hai, việc phát hiện near-duplicate được kế thừa từ Silver và phụ thuộc thuật toán blocking. Một số block lớn không được so sánh đầy đủ ở Notebook 02; do vậy chưa thể bảo đảm phát hiện 100% near-duplicate.

Thứ ba, Notebook 03 không thực hiện đánh giá ngữ nghĩa/factuality toàn bộ 678.152 bản ghi. Các kết quả suitability ở giai đoạn trước có hỗ trợ AI, nên không được trình bày như toàn bộ reference đã được chuyên gia đánh giá độc lập.

Thứ tư, Notebook 03 chưa tokenization, chưa thiết lập input/output token budget và chưa truncation. Vì vậy **chưa thể khẳng định toàn bộ 678.152 cặp Gold sẽ được mô hình tiếp nhận nguyên vẹn**.

Thứ năm, chất lượng Gold chưa phải chất lượng mô hình. Các chỉ số ROUGE, BERTScore, factual consistency, latency và chi phí huấn luyện phải được đo ở giai đoạn thực nghiệm sau, không suy ra từ kết quả làm sạch dữ liệu.

Cuối cùng, việc chỉ sử dụng VietNews cho Super khiến nghiên cứu chưa có một bộ reference tiếng Việt chuyên biệt cho Normal. Đây là giới hạn phạm vi được chủ động lựa chọn để giảm khối lượng huấn luyện.

## 9. Định hướng bước tiếp theo

Gold tạo ra ba nguồn dữ liệu rõ ràng cho các thực nghiệm Scratch Transformer Direct, Scratch Transformer Evidence-aware, Qwen Fine-tuning Direct và Qwen Fine-tuning Evidence-aware. Với ba dataset–task, ma trận dự kiến gồm 12 cấu hình huấn luyện; đây là **kế hoạch**, chưa phải 12 lần training đã thực hiện.

Trước huấn luyện, cần chạy thử tokenizer trên mẫu đại diện của cả ba tập; đo phân phối số token thực tế, tỷ lệ vượt giới hạn context và output; thiết kế chính sách truncation/chunking có kiểm soát; chuẩn bị evidence extraction; khóa giao thức đánh giá với test không tham gia chọn mô hình. Đồng thời cần đọc kiểm chứng trực tiếp Gold Parquet từ máy chạy pipeline để xác nhận schema và nội dung mẫu.

## 10. Kết luận

Notebook 03 đã hoàn thiện giai đoạn xây dựng **Gold Training Pairs** cho hệ thống HybriNews trên ba bộ dữ liệu CNN/DailyMail, XSum và VietNews. Từ **678.647 bản ghi Silver**, quá trình precheck phát hiện **495 bản ghi** mang ít nhất một dấu hiệu rủi ro về độ dài, tính đầy đủ hoặc biểu diễn văn bản. Theo quyết định lọc thận trọng của nhóm, các bản ghi này được loại sau bước phê duyệt, tạo ra **678.152 bản ghi Gold**, gồm **586.049 train, 46.837 validation và 45.266 test**. Số liệu giữa bảng tổng hợp và Gold commit manifest thống nhất, và manifest ghi nhận trạng thái **COMPLETE**.

Bộ dữ liệu Gold được tổ chức thành **ba cấu hình nhiệm vụ**: CNN/DailyMail cho **English Normal Summarization**, XSum cho **English Super Summarization** và VietNews cho **Vietnamese Super Summarization**. Mỗi source giữ một reference đã chọn từ quá trình làm sạch trước đó; các trường ID, hash, split và văn bản gốc được bảo toàn để hỗ trợ truy vết. Đặc biệt, VietNews giữ biểu diễn tách từ có dấu `_` đồng thời cung cấp phiên bản văn bản tự nhiên cho mô hình. Notebook không sinh thêm reference và chưa thực hiện tokenizer hoặc cắt ngắn nội dung.

Về mặt quy trình, việc tách **Giai đoạn A – đánh giá trước** và **Giai đoạn B – áp dụng quyết định, kiểm định rồi ghi Gold** giúp tránh tự động đưa dữ liệu nghi vấn vào huấn luyện. Tuy nhiên, nhóm cần công bố rằng việc loại 495 bản ghi là một chính sách lựa chọn dữ liệu, không phải xác nhận tất cả đều bị lỗi; đồng thời các giới hạn về near-duplicate, factuality và token budget vẫn chưa được giải quyết hoàn toàn. **Nhìn chung, Notebook 03 đã hoàn thành mục tiêu chuẩn bị và công bố Gold theo kết quả pipeline, tạo nền tảng dữ liệu có cấu trúc, có kiểm soát và có khả năng truy vết cho những thí nghiệm tóm tắt tiếp theo của HybriNews.**
