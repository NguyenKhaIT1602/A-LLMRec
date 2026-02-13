# import os
# import os.path
# import gzip
# import json
# import pickle
# from tqdm import tqdm
# from collections import defaultdict

# def parse(path):
#     g = gzip.open(path, 'rb')
#     for l in tqdm(g):
#         yield json.loads(l)
        
# def preprocess(fname):
#     countU = defaultdict(lambda: 0)
#     countP = defaultdict(lambda: 0)
#     line = 0

#     file_path = f'../../data/amazon/{fname}.json.gz'
    
#     # counting interactions for each user and item
#     for l in parse(file_path):
#         line += 1
#         if ('Beauty' in fname) or ('Toys' in fname):
#             if l['overall'] < 3:
#                 continue
#         asin = l['asin']
#         rev = l['reviewerID']
#         time = l['unixReviewTime']

#         countU[rev] += 1
#         countP[asin] += 1
    
#     usermap = dict()
#     usernum = 0
#     itemmap = dict()
#     itemnum = 0
#     User = dict()
#     review_dict = {}
#     name_dict = {'title':{}, 'description':{}}
    
#     f = open(f'../../data/amazon/meta_{fname}.json', 'r')
#     json_data = f.readlines()
#     f.close()
#     data_list = [json.loads(line[:-1]) for line in json_data]
#     meta_dict = {}
#     for l in data_list:
#         meta_dict[l['asin']] = l
    
#     for l in parse(file_path):
#         line += 1
#         asin = l['asin']
#         rev = l['reviewerID']
#         time = l['unixReviewTime']
        
#         threshold = 5
#         if ('Beauty' in fname) or ('Toys' in fname):
#             threshold = 4
            
#         if countU[rev] < threshold or countP[asin] < threshold:
#             continue
        
#         if rev in usermap:
#             userid = usermap[rev]
#         else:
#             usernum += 1
#             userid = usernum
#             usermap[rev] = userid
#             User[userid] = []
        
#         if asin in itemmap:
#             itemid = itemmap[asin]
#         else:
#             itemnum += 1
#             itemid = itemnum
#             itemmap[asin] = itemid
#         User[userid].append([time, itemid])
        
        
#         if itemmap[asin] in review_dict:
#             try:
#                 review_dict[itemmap[asin]]['review'][usermap[rev]] = l['reviewText']
#             except:
#                 a = 0
#             try:
#                 review_dict[itemmap[asin]]['summary'][usermap[rev]] = l['summary']
#             except:
#                 a = 0
#         else:
#             review_dict[itemmap[asin]] = {'review': {}, 'summary':{}}
#             try:
#                 review_dict[itemmap[asin]]['review'][usermap[rev]] = l['reviewText']
#             except:
#                 a = 0
#             try:
#                 review_dict[itemmap[asin]]['summary'][usermap[rev]] = l['summary']
#             except:
#                 a = 0
#         try:
#             if len(meta_dict[asin]['description']) ==0:
#                 name_dict['description'][itemmap[asin]] = 'Empty description'
#             else:
#                 name_dict['description'][itemmap[asin]] = meta_dict[asin]['description'][0]
#             name_dict['title'][itemmap[asin]] = meta_dict[asin]['title']
#         except:
#             a =0
    
#     with open(f'../../data/amazon/{fname}_text_name_dict.json.gz', 'wb') as tf:
#         pickle.dump(name_dict, tf)
    
#     for userid in User.keys():
#         User[userid].sort(key=lambda x: x[0])
        
#     print(usernum, itemnum)
    
#     f = open(f'../../data/amazon/{fname}.txt', 'w')
#     for user in User.keys():
#         for i in User[user]:
#             f.write('%d %d\n' % (user, i[1]))
#     f.close()

import os
import json
import pickle
from tqdm import tqdm
from collections import defaultdict

def preprocess_kaggle(fname):
    countU = defaultdict(lambda: 0)
    countP = defaultdict(lambda: 0)
    line_count = 0

    # 1. Cấu hình đường dẫn trên Kaggle
    input_dir = "/kaggle/input/DataAI"
    output_dir = "/kaggle/working"
    
    review_file = os.path.join(input_dir, f"{fname}.json")
    meta_file = os.path.join(input_dir, f"meta_{fname}.json")
    
    # 2. Đếm số tương tác (Đọc file JSON thuần, không dùng gzip)
    print(f"--- Đang đọc file review: {review_file} ---")
    with open(review_file, 'r') as f:
        for line in tqdm(f):
            l = json.loads(line)
            # Lọc theo overall cho Beauty và Toys (nếu cần)
            if ('Beauty' in fname) or ('Toys' in fname):
                if l.get('overall', 0) < 3:
                    continue
            
            countU[l['reviewerID']] += 1
            countP[l['asin']] += 1

    # 3. Đọc dữ liệu Meta (Metadata)
    print(f"--- Đang đọc file meta: {meta_file} ---")
    meta_dict = {}
    with open(meta_file, 'r') as f:
        for line in tqdm(f):
            try:
                # Một số file meta có thể bị lỗi dòng cuối hoặc định dạng, dùng try-except cho an toàn
                l = json.loads(line)
                meta_dict[l['asin']] = l
            except:
                continue

    # 4. Xử lý chính
    usermap = dict()
    usernum = 0
    itemmap = dict()
    itemnum = 0
    User = dict()
    name_dict = {'title': {}, 'description': {}}
    
    threshold = 4 if ('Beauty' in fname) or ('Toys' in fname) else 5

    print("--- Đang xử lý mapping và tạo tập tin văn bản ---")
    with open(review_file, 'r') as f:
        for line in tqdm(f):
            l = json.loads(line)
            asin = l['asin']
            rev = l['reviewerID']
            time = l['unixReviewTime']

            if countU[rev] < threshold or countP[asin] < threshold:
                continue

            if rev not in usermap:
                usernum += 1
                usermap[rev] = usernum
                User[usernum] = []
            
            userid = usermap[rev]

            if asin not in itemmap:
                itemnum += 1
                itemmap[asin] = itemnum
                
                # Lưu thông tin text cho item
                if asin in meta_dict:
                    meta_item = meta_dict[asin]
                    name_dict['title'][itemnum] = meta_item.get('title', 'No Title')
                    desc = meta_item.get('description', [])
                    name_dict['description'][itemnum] = desc[0] if (isinstance(desc, list) and len(desc) > 0) else 'Empty description'
                else:
                    name_dict['title'][itemnum] = 'No Title'
                    name_dict['description'][itemnum] = 'Empty description'

            itemid = itemmap[asin]
            User[userid].append([time, itemid])

    # 5. Lưu kết quả ra /kaggle/working
    print(f"--- Đang lưu kết quả. User: {usernum}, Item: {itemnum} ---")
    
    # Lưu file pickle chứa name_dict
    output_pickle = os.path.join(output_dir, f'{fname}_text_name_dict.pkl')
    with open(output_pickle, 'wb') as f:
        pickle.dump(name_dict, f)
    
    # Sắp xếp và lưu file tương tác .txt
    output_txt = os.path.join(output_dir, f'{fname}.txt')
    with open(output_txt, 'w') as f:
        for userid in User.keys():
            User[userid].sort(key=lambda x: x[0])
            for interaction in User[userid]:
                f.write(f'{userid} {interaction[1]}\n')

    print(f"Hoàn thành! File đã lưu tại: {output_dir}")

# Chạy thử với Movies_and_TV
preprocess_kaggle("Movies_and_TV")