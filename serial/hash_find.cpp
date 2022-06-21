//
// Created by 田佳业 on 2022/6/5.
//

#include <iostream>
#include "../readdata.h"

using namespace std;
POSTING_LIST *posting_list_container = (struct POSTING_LIST *) malloc(POSTING_LIST_NUM * sizeof(struct POSTING_LIST));
POSTING_LIST *posting_list_hash_container = (struct POSTING_LIST *) malloc(
        POSTING_LIST_NUM * sizeof(struct POSTING_LIST));
vector<vector<int> > query_list_container;
MyTimer time_get_intersection;
MyTimer time_get_hash_table;
const int K = 25;
int P = 10;

int QueryNum = 500;


void get_sorted_index(POSTING_LIST *queried_posting_list, int query_word_num, int *sorted_index) {

    for (int i = 0; i < query_word_num; i++) {
        sorted_index[i] = i;
    }
    for (int i = 0; i < query_word_num - 1; i++) {
        for (int j = i + 1; j < query_word_num; j++) {
            if (queried_posting_list[sorted_index[i]].len > queried_posting_list[sorted_index[j]].len) {
                int temp = sorted_index[i];
                sorted_index[i] = sorted_index[j];
                sorted_index[j] = temp;
            }
        }
    }
}


bool hash_find(POSTING_LIST *list, POSTING_LIST *hash_list, unsigned int element) {
    unsigned int hash_key = element >> P;
    if (hash_list->arr[hash_key] == -1)
        return false;
    else {
        int index = hash_list->arr[hash_key];
        for (int i = index; i < list->len; i++) {
            if (list->arr[i] > element)
                return false;
            if (list->arr[i] == element)
                return true;
        }

    }
}

bool binary_search(POSTING_LIST *list, unsigned int element) {
    int low = 0, high = list->len - 1, mid;
    while (low <= high) {
        mid = (low + high) / 2;
        if (list->arr[mid] == element)
            return true;
        else if (list->arr[mid] < element)
            low = mid + 1;
        else
            high = mid - 1;
    }
    return false;
}


void simplified_Adp(POSTING_LIST *queried_posting_list, POSTING_LIST *queried_posting_list_hash, int query_word_num,
                    vector<unsigned int> &result_list) {
    //start with sorting the posting list to find the shortest one
    int *sorted_index = new int[query_word_num];
    get_sorted_index(queried_posting_list, query_word_num, sorted_index);
    bool flag;
    unsigned int key_element;
    for (int k = 0; k < queried_posting_list[sorted_index[0]].len; k++) {
        flag = true;
        key_element = queried_posting_list[sorted_index[0]].arr[k];
        for (int m = 1; m < query_word_num; m++) {
            int mth_short = sorted_index[m];
            POSTING_LIST searching_list = queried_posting_list[mth_short];
            POSTING_LIST searching_list_hash = queried_posting_list_hash[mth_short];
            //if the key element is larger than the end element of a list ,it means any element larger than the key element can not be the intersection
            if (key_element > searching_list.arr[searching_list.len - 1]) {
                goto end;
            }
            if(!hash_find(&searching_list,&searching_list_hash,key_element)){
                flag = false;
                break;
            }
        }
        if (flag) {
            result_list.push_back(key_element);
        }
    }
    end:
    delete[] sorted_index;
}


void query_starter(vector<vector<unsigned int>> &hash_result) {

    time_get_intersection.start();
    for (int i = 0; i < QueryNum; i++) {
        int query_word_num = query_list_container[i].size();
        //get the posting list of ith query
        auto *queried_posting_list = new POSTING_LIST[query_word_num];
        auto *queried_posting_list_hash = new POSTING_LIST[query_word_num];
        for (int j = 0; j < query_word_num; j++) {
            int query_list_item = query_list_container[i][j];
            queried_posting_list[j] = posting_list_container[query_list_item];
            queried_posting_list_hash[j] = posting_list_hash_container[query_list_item];
        }
        //get the result of ith query
        vector<unsigned int> hash_result_list;
        simplified_Adp(queried_posting_list, queried_posting_list_hash, query_word_num, hash_result_list);
        hash_result.push_back(hash_result_list);
        hash_result_list.clear();
        delete[] queried_posting_list;
    }
    time_get_intersection.finish();
}

void get_hash_table(POSTING_LIST *posting_list_hash_container, POSTING_LIST *posting_list_container) {
    //The hash role is x>>p
    //So the docID of a document within the same segment differs only in the lowest p bits of the docID.
    //the hash table stores the segments of the posting list and the position of the first element of the segment.
    time_get_hash_table.start();
    for (int i = 0; i < POSTING_LIST_NUM; i++) {
        //get the length of hash table according to the last element of the posting list
        posting_list_hash_container[i].len =
                (posting_list_container[i].arr[posting_list_container[i].len - 1] >> P);
        posting_list_hash_container[i].arr = new unsigned int[posting_list_hash_container[i].len];
        for (int j = 0; j < posting_list_hash_container[i].len; j++) {
            posting_list_hash_container[i].arr[j] = -1;
        }
        //-1 means the hash table is empty
        unsigned int hash_key;
        for (int j = 0; j < posting_list_container[i].len; j++) {
            hash_key = posting_list_container[i].arr[j] >> P;
            if (posting_list_hash_container[i].arr[hash_key] == -1) {
                posting_list_hash_container[i].arr[hash_key] = j;
            }
        }
    }
    time_get_hash_table.finish();
}

int main() {

    if (read_posting_list(posting_list_container) || read_query_list(query_list_container)) {
        printf("read_posting_list failed\n");
        free(posting_list_container);
        return -1;
    } else {
        printf("query_num: %d\n", QueryNum);
        get_hash_table(posting_list_hash_container, posting_list_container);
        //test the hash table
//        for (int i = 0; i < 5; i++) {
//            printf("%d:", i);
//            for (int j = 0; j < posting_list_hash_container[i].len + 1; j++) {
//                printf("%d ", posting_list_hash_container[i].arr[j]);
//            }
//            printf("\n");
//        }
        vector<vector<unsigned int>> hash_result;
        query_starter(hash_result);
        //test the correctness of the result
        for (int j = 0; j < 5; ++j) {
            printf("result %d: ", j);
            printf("%zu\n", hash_result[j].size());
            for (int k = 0; k < hash_result[j].size(); ++k) {
                printf("%d ", hash_result[j][k]);
            }
            printf("\n");
        }
        time_get_hash_table.get_duration("get_hash_table");
        time_get_intersection.get_duration("hash_table simplified_Adp");
        free(posting_list_container);
        return 0;
    }
}


