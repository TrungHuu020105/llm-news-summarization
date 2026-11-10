# BÁO CÁO KẾT QUẢ NOTEBOOK 02 — SILVER DATA CLEANING

**Dự án:** HybriNews — Hệ thống tóm tắt tin tức  
**Giai đoạn:** Chuẩn hóa, làm sạch, xử lý trùng lặp và kiểm soát chất lượng dữ liệu Silver  
**Phạm vi:** CNN/DailyMail, XSum và VietNews  
**Nguồn số liệu:** `after_review/final_cleaning_summary.csv`, ba file `reviewed_cleaning_summary.json` và `silver_commit_manifest.json` được xuất từ pipeline; đối chiếu logic với Notebook 02 Giai đoạn B đã thiết kế.  
**Thời điểm ghi manifest:** 10/11/2026, 06:47:38 UTC (13:47:38 giờ Việt Nam).

## 1. Mục tiêu và quy trình

Notebook 02 chuyển dữ liệu Bronze thành dữ liệu Silver qua các bước: chuẩn hóa Unicode NFC và khoảng trắng, bảo toàn biểu diễn word segmentation có dấu `_` của VietNews; loại source hoặc target rỗng; giữ một bản đại diện trong các trường hợp trùng hoàn toàn source–target; ưu tiên split `test > validation > train` khi một source xuất hiện xuyên split; áp dụng phiếu chọn reference đối với cùng source khác target; áp dụng kết quả rà soát near-duplicate; cuối cùng kiểm tra và ghi Parquet.

Các dữ liệu bị loại được ghi lại theo lý do để có thể truy vết. Bronze được giữ nguyên. Với near-duplicate, quy trình tìm cặp ứng viên theo blocking và kiểm tra Jaccard không bảo đảm phát hiện mọi cặp gần trùng.

## 2. Kết quả tổng quan

| Dataset | Bronze | Sau bước xử lý tự động | Loại khi áp dụng review | Silver cuối cùng | Tỷ lệ giữ lại |
|---|---:|---:|---:|---:|---:|
| CNN/DailyMail | 311.971 | 308.867 | 418 | 308.449 | 98,87% |
| XSum | 226.711 | 226.521 | 108 | 226.413 | 99,87% |
| VietNews | 143.816 | 143.815 | 30 | 143.785 | 99,98% |
| **Tổng** | **682.498** | **679.203** | **556** | **678.647** | **99,44%** |

Tổng cộng **3.851 bản ghi đã bị loại**, tương đương **0,56%** dữ liệu Bronze thuộc ba dataset được chọn. Tổng số dòng Silver bằng đúng tổng số dòng còn lại sau lọc tự động trừ số dòng loại ở bước review: **679.203 − 556 = 678.647**.

## 3. Phân tích riêng từng dataset

### 3.1. CNN/DailyMail — English Normal Summarization

| Lý do loại | Số bản ghi |
|---|---:|
| Trùng hoàn toàn source–target | 3.103 |
| Trùng source xuyên split | 1 |
| Reference không được chọn trong nhóm có nhiều target | 9 |
| Near-duplicate theo quyết định review | 409 |
| **Tổng bị loại** | **3.522** |

CNN/DailyMail giảm từ **311.971 xuống 308.449 bản ghi**, giữ lại **98,87%**. Không phát hiện source/target rỗng theo quy tắc kiểm tra. Phần lớn số bản ghi bị loại là duplicate hoàn toàn; điều này cho thấy bước khử trùng lặp có ý nghĩa lớn hơn so với lọc bản ghi rỗng trong dataset này. Sau khi áp dụng review, 9 reference không được chọn và 409 source near-duplicate được loại theo chính sách đã chốt.

Đầu ra có **283.611 train**, **13.361 validation** và **11.477 test**.

### 3.2. XSum — English Super Summarization

| Lý do loại | Số bản ghi |
|---|---:|
| Source/target rỗng | 34 |
| Trùng hoàn toàn source–target | 103 |
| Trùng source xuyên split | 53 |
| Source dạng boilerplate không đủ nội dung | 53 |
| Near-duplicate theo quyết định review | 52 |
| Reference không được chọn | 3 |
| **Tổng bị loại** | **298** |

XSum giảm từ **226.711 xuống 226.413 bản ghi**, giữ lại **99,87%**. Trong 23 nhóm source có nhiều target, nhóm nghiên cứu đã quyết định loại toàn bộ **20 nhóm source boilerplate**; tại thời điểm áp dụng review, các nhóm này tương ứng **53 bản ghi** cần loại. Ba nhóm còn lại được giữ một target đại diện mỗi nhóm, loại ba reference không được chọn. Động cơ loại boilerplate là source không cung cấp đủ nội dung tin tức để tạo một cặp source–summary đáng tin cậy, không phải vì độ dài source ngắn đơn thuần.

Đầu ra có **203.766 train**, **11.321 validation** và **11.326 test**. Đây là dataset có số bản ghi bị loại vì trùng source xuyên split nhiều nhất trong ba bộ dữ liệu (**53 bản ghi**).

### 3.3. VietNews — Vietnamese Super Summarization

| Lý do loại | Số bản ghi |
|---|---:|
| Trùng source xuyên split | 1 |
| Near-duplicate theo quyết định review | 30 |
| **Tổng bị loại** | **31** |

VietNews giảm từ **143.816 xuống 143.785 bản ghi**, giữ lại **99,98%**. Báo cáo không ghi nhận source/target rỗng, duplicate source–target hoàn toàn hoặc nhóm source có nhiều target cần lựa chọn. Có một bản ghi trùng source xuyên split được xử lý ở quy trình Silver và 30 source near-duplicate bị loại theo phiếu review.

Đầu ra có **99.105 train**, **22.182 validation** và **22.498 test**. Theo manifest, biểu diễn word segmentation bằng dấu `_` được bảo toàn, tránh làm mất thông tin đặc thù của dữ liệu tiếng Việt.

## 4. Quy mô dữ liệu Silver theo split

| Dataset | Train | Validation | Test | Tổng |
|---|---:|---:|---:|---:|
| CNN/DailyMail | 283.611 | 13.361 | 11.477 | 308.449 |
| XSum | 203.766 | 11.321 | 11.326 | 226.413 |
| VietNews | 99.105 | 22.182 | 22.498 | 143.785 |
| **Tổng** | **586.482** | **46.864** | **45.301** | **678.647** |

Số dòng theo từng split cộng lại khớp với tổng Silver của cả ba dataset. Tập test được giữ theo chính sách ưu tiên split đã định nghĩa, nhưng những bản ghi bị loại khỏi test do các lý do chất lượng khác vẫn phải được công bố để tránh hiểu nhầm rằng kích thước tập test hoàn toàn không thay đổi.

## 5. Kiểm tra tính nhất quán và trạng thái đầu ra

Tổng số dòng bị loại theo từng lý do bằng chênh lệch Bronze–Silver: **3.522** đối với CNN/DailyMail, **298** đối với XSum và **31** đối với VietNews. Tất cả ba file `reviewed_cleaning_summary.json` đều khớp với `final_cleaning_summary.csv` và các `split_counts` trong manifest.

File `silver_commit_manifest.json` ghi `status = COMPLETE` và chỉ tới ba thư mục Parquet Silver trên máy thực thi. Mã Notebook 02 Giai đoạn B được thiết kế để kiểm tra số dòng khi ghi tạm, kiểm tra source hash duy nhất và source/target không rỗng, đối chiếu phiếu review, rồi mới chuyển dữ liệu tạm thành Silver chính thức.

**Giới hạn xác minh:** File ZIP được cung cấp chỉ chứa các báo cáo và Parquet lưu dấu vết bản ghi bị loại, **không chứa ba thư mục `data/silver/`**. Do đó, phân tích này xác nhận tính nhất quán của báo cáo/manifest, nhưng **chưa kiểm tra độc lập trực tiếp các file Silver Parquet**. File `.ipynb` đính kèm cũng là bản Giai đoạn A 51 cell, không chứa output thực thi và cell Giai đoạn B; nên chưa thể xác nhận lại log hoàn thành chỉ từ notebook đính kèm.

## 6. Hạn chế và kiểm tra tiếp theo

Cơ chế near-duplicate chỉ tạo ứng viên dựa trên blocking; báo cáo ghi **29 block bỏ qua với CNN/DailyMail**, **20 với XSum**, **0 với VietNews**. Như vậy, việc các cặp được chấm đã xử lý xong không đồng nghĩa mọi near-duplicate trong corpus đều đã được phát hiện. Báo cáo cũng chưa chứng minh đầy đủ khả năng không có near-duplicate giữa các dataset khác nhau, hay kiểm định độc lập chất lượng reference trên toàn tập.

Trước khi chuyển Gold, cần dùng dữ liệu Parquet Silver thực tế để tái kiểm tra schema, độ rỗng, source hash duy nhất trong từng dataset và phân bố split; khóa các file review, manifest và phiên bản quy tắc normalization. Với XSum, cần bảo đảm 20 source hash boilerplate không xuất hiện trong Silver. Đối với tập test, duy trì nguyên tắc không sử dụng dữ liệu test để tối ưu mô hình hoặc hyperparameter.

## 7. Kết luận

Notebook 02 đã hoàn thành quá trình làm sạch được báo cáo cho **ba bộ dữ liệu CNN/DailyMail, XSum và VietNews**, giảm từ **682.498 xuống 678.647 bản ghi**, loại **3.851 bản ghi (0,56%)** và giữ lại **99,44%** quy mô dữ liệu đầu vào. Các phép xử lý không chỉ loại dữ liệu rỗng và trùng hoàn toàn mà còn áp dụng chính sách split nhất quán, lựa chọn một reference cho mỗi source có nhiều target và sử dụng kết quả rà soát near-duplicate. Đáng chú ý, **20 nhóm source boilerplate của XSum đã được loại bỏ hoàn toàn** theo quyết định của nhóm nhằm hạn chế cặp huấn luyện thiếu căn cứ thông tin.

Dữ liệu Silver được tổ chức theo ba cấu hình đã chốt: **CNN/DailyMail — English Normal**, **XSum — English Super** và **VietNews — Vietnamese Super**. Kết quả này tạo nền tảng cho Notebook 03 xây dựng cặp huấn luyện Gold và tiếp tục kiểm soát chất lượng dữ liệu. Tuy nhiên, kết luận về khả năng loại bỏ near-duplicate chỉ có hiệu lực trong phạm vi các ứng viên được thuật toán phát hiện và review; việc xác nhận Parquet Silver cuối cùng cần thực hiện trực tiếp trên các thư mục đầu ra trong môi trường chạy của dự án.
