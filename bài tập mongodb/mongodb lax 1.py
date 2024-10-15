from calendar import month

from pymongo import MongoClient
from datetime import datetime
#bước 1: kết nối tới MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['facebookpy'] #chọn cơ sở dữ liệu tiktok
#bước 2: tạo các colections
users_collection = db['users']
posts_collection = db['posts']
#### buoc 3 the, du leu nguoi dung
user_data = [
    { 'user_id': 1, 'name': "Nguyen Van A", 'email': "a@gmail.com", 'age': 25 },
    { 'user_id': 2, 'name': "Tran Thi B", 'email': "b@gmail.com", 'age': 30 },
    { 'user_id': 3, 'name': "Le Van C", 'email': "c@gmail.com", 'age': 22 }
]
users_collection.insert_many(user_data)
posts_data = [
    { 'comment_id': 1, 'post_id': 1, 'user_id': 2, 'content': "Thật tuyệt vời!", 'created_at': 'datetime("2024-10-01")' },
    { 'comment_id': 2, 'post_id': 2, 'user_id': 3, 'content': "Mình cũng muốn xem bộ phim này!", 'created_at': 'datetime("2024-10-02")' },
    { 'comment_id': 3, 'post_id': 3, 'user_id': 1, 'content': "Cảm ơn bạn!", 'created_at': 'datetime("2024-10-03")' }
]
posts_collection.insert_many(posts_data)
#bước 5: truy vấn dữ liệu
#5.1 xem tất cả dữ liệu người dùng
print("Tất cả người dùng")
for user in users_collection.find():
    print(user)

#5.3 tìm tất cả psot  của người dùng có post la post 1
print("\n Tất cả post của người dùng 'post 1': ")
user_post = users_collection.find({'user_post': 1})
for post in user_post:
    print(user_id)

#5.3 tìm tất cả video của người dùng có username là user1
print("\n Tất cả post của người dùng 'post 1': ")
user_user = users_collection.find({'user_user': 1})
for user in user_user:
    print(user_id)

#### truy van nguooi dung cos do tuoi tren 25
print("\n nguoi dung co do tuoi tren 25 ' user lon hon 25': " )
user_user = users_collection.find({'user_age':{'$gte': 25}} )
for user in user_user:
    print(user_age)

#### truy van nguooi dung cos do tuoi tren 25
print("\n bai dang duoc tao thang 10 ' bai dang duoc tao thang 10 ': " )
user_user = users_collection.find({'user_postduoctao':{'$gte':datetime(year=2024 , month=10 ,day=1), '$lt':datetime(year=2024 , month=10 , day=1 )}} )
for user in user_user:
    print(user_createat)


#### cap nhat
users_collection.update_one({'user_id': 1},{'$set':{'content': 'thoi tiet that dep '}})


######### xoa comment
posts_collection.delete_one({'comment_id': 2})

#bước 8: xem lại dữ liệu sau khi cập nhật và xóa
print("\n Dữ liệu người dùng sau khi cập nhật: ")
for user in users_collection.find():
    print(user)
print("\n Dữ liệu video sau khi xóa: ")
for post in posts_collection.find():
    print(post)
#đóng kết nối
client.close()