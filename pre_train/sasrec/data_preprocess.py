import os
import json
import pickle
from tqdm import tqdm
from collections import defaultdict

def preprocess(fname):
    countU = defaultdict(lambda: 0)
    countP = defaultdict(lambda: 0)
    line_count = 0

    # 1. Cấu hình đường dẫn trên Kaggle
    input_dir_data = "/kaggle/input/dataai/Movies_and_TV.json"
    input_dir_meta = "/kaggle/input/dataai/meta_Movies_and_TV.json"

    output_dir = "/kaggle/working"
    
    review_file = os.path.join(input_dir_data, f"{fname}.json")
    meta_file = os.path.join(input_dir_meta, f"meta_{fname}.json")
    
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