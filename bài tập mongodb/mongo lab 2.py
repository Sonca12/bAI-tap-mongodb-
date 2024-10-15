from calendar import month

from pymongo import MongoClient
from datetime import datetime
#bước 1: kết nối tới MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['diverpy'] #chọn cơ sở dữ liệu gg diver
#bước 2: tạo các colections
file_collection = db['file']
file_data = [
    { 'file_id': 1, 'name': "Report.pdf", 'size': 2048, 'owner': "Nguyen Van A", 'created_at': datetime(2024,1,10), 'shared': 'false' },
    { 'file_id': 2, 'name': "Presentation.pptx", 'size': 5120, 'owner': "Tran Thi B", 'created_at': datetime(2024,1,15), 'shared': 'true' },
    { 'file_id': 3, 'name': "Image.png", 'size': 1024, 'owner': "Le Van C", 'created_at': datetime(2024,1,20), 'shared': 'false' },
    { 'file_id': 4, 'name': "Spreadsheet.xlsx", 'size': 3072, 'owner': "Pham Van D", 'created_at': datetime(2024,1,25), 'shared': 'true' },
    { 'file_id': 5, 'name': "Notes.txt", 'size': 512, 'owner': "Nguyen Thi E", 'created_at': datetime(2024,1,30), 'shared': 'false' }

]
file_collection.insert_many(file_data)
#bước 5: truy vấn dữ liệu
#5.1 xem tất cả dữ liệu người dùng
print("Tất cả file ")
for file in file_collection.find():
    print(file)

#### truy van nguooi dung cos do tuoi tren 25
print("\n size tren 2000 ' kich thuoc tren 2000': " )
file_file = file_collection.find({'file_size':{'$gte': 2000}} )
for file in file_file:
    print(user_size)

#################### dem tong so tep
count = file_collection.count_documents({})
print("Tổng số file:", count)

# Tìm tất cả các tệp được chia sẻ
shared_files = file_collection.find({"shared": True})

# In danh sách các tệp được chia sẻ
print("Các tệp được chia sẻ:")
for file in shared_files:
    print(file)

# Thống kê số lượng tệp theo chủ sở hữu
pipeline = [
    {
        "$group": {
            "_id": "$owner",  # Nhóm theo trường owner
            "file_count": { "$sum": 1 }  # Đếm số lượng tệp
        }
    }
]

# Thực hiện truy vấn
result = file_collection.aggregate(pipeline)

# In kết quả
print("Số lượng tệp theo chủ sở hữu:")
for owner in result:
    print(f"Chủ sở hữu: {owner['_id']}, Số lượng tệp: {owner['file_count']}")

# Cập nhật trạng thái chia sẻ của tệp với file_id = 1 thành true
result = file_collection.update_one(
    {"file_id": 1},  # Điều kiện tìm kiếm
    {"$set": {"shared": True}}  # Cập nhật trạng thái chia sẻ
)

# Kiểm tra xem có tài liệu nào được cập nhật hay không
if result.modified_count > 0:
    print("Cập nhật thành công!")
else:
    print("Không có tệp nào được cập nhật.")

# Xóa tệp với file_id = 3
delete_result = file_collection.delete_one({"file_id": 3})

# Kiểm tra xem có tệp nào được xóa hay không
if delete_result.deleted_count > 0:
    print("Xóa tệp thành công!")
else:
    print("Không tìm thấy tệp để xóa.")

# Lấy tất cả các tệp trong collection
all_files = file_collection.find()

# In danh sách các tệp
print("Danh sách tất cả tệp:")
for file in all_files:
    print(file)






    ####################################################### quétion 2

#Tìm tất cả tệp của người dùng "Nguyen Van A"
user_files = file_collection.find({"owner": "Nguyen Van A"})

# In danh sách các tệp của người dùng
print("Các tệp của người dùng 'Nguyen Van A':")
for file in user_files:
    print(file)

# Tìm tệp lớn nhất trong bộ sưu tập
largest_file = file_collection.find_one(sort=[("size", -1)])  # Sắp xếp theo kích thước giảm dần

# In thông tin về tệp lớn nhất
if largest_file:
    print("Tệp lớn nhất:")
    print(largest_file)
else:
    print("Không tìm thấy tệp nào trong bộ sưu tập.")


# Đếm số lượng tệp có kích thước nhỏ hơn 1000KB
count = file_collection.count_documents({"size": {"$lt": 1000}})  # 1000KB

# In kết quả
print("Số lượng tệp có kích thước nhỏ hơn 1000KB:", count)


# Định nghĩa khoảng thời gian cho tháng 1 năm 2024
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 2, 1)

# Tìm tất cả tệp được tạo trong tháng 1 năm 2024
files_in_january = file_collection.find({"created_at": {"$gte": start_date, "$lt": end_date}})

# In danh sách các tệp
print("Các tệp được tạo trong tháng 1 năm 2024:")
for file in files_in_january:
    print(file)

# Cập nhật tên tệp với file_id = 4 thành "New Spreadsheet.xlsx"
result = file_collection.update_one(
    {"file_id": 4},  # Điều kiện tìm kiếm
    {"$set": {"name": "New Spreadsheet.xlsx"}}  # Cập nhật tên tệp
)

# Kiểm tra xem có tài liệu nào được cập nhật hay không
if result.modified_count > 0:
    print("Cập nhật tên tệp thành công!")
else:
    print("Không có tệp nào được cập nhật.")

# Xóa tất cả tệp có kích thước nhỏ hơn 1000KB
delete_result = file_collection.delete_many({"size": {"$lt": 1000}})  # 1000KB

# In kết quả
print(f"Đã xóa {delete_result.deleted_count} tệp có kích thước nhỏ hơn 1000KB.")