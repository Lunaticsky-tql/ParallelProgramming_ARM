//
// Created by 田佳业 on 2022/5/1.
//

#include <iostream>
#include "../readdata.h"
//using ptread to parallel the process of finding the key elements

using namespace std;
POSTING_LIST *posting_list_container = (struct POSTING_LIST *) malloc(POSTING_LIST_NUM * sizeof(struct POSTING_LIST));
vector<vector<int> > query_list_container;
MyTimer time_get_intersection;

int QueryNum = 500;
const int THREAD_NUM = 8;
typedef struct {

    int thread_id;
    POSTING_LIST *list;
    int query_word_num;
    int *sorted_index;
    int finding_elements;
    int start_pos;
} block_Parm;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
vector<vector<unsigned int>> temp_result_list(THREAD_NUM, vector<unsigned int>());
pthread_t thread[THREAD_NUM];

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

int binary_search_with_position(POSTING_LIST *list, unsigned int element, int index) {
    //If found, return the position of the element; otherwise, return the position not less than its first element
    int low = index, high = list->len - 1, mid;
    while (low <= high) {
        mid = (low + high) / 2;
        if (list->arr[mid] == element)
            return mid;
        else if (list->arr[mid] < element)
            low = mid + 1;
        else
            high = mid - 1;
    }
    return low;
}

void *parallel_finding_element(void *task) {
    int thread_id = ((block_Parm *) task)->thread_id;
    POSTING_LIST *list = ((block_Parm *) task)->list;
    int query_word_num = ((block_Parm *) task)->query_word_num;
    int *sorted_index = ((block_Parm *) task)->sorted_index;
    int finding_elements = ((block_Parm *) task)->finding_elements;
    int start_pos= ((block_Parm *) task)->start_pos;
    vector<int> finding_pointer(query_word_num, 0);
    vector<unsigned int> result_in_thread;
    bool flag;
    unsigned int key_element;
    for(int k=start_pos;k<start_pos+finding_elements;k++){
        flag= true;
        key_element = list[sorted_index[0]].arr[k];
        for(int m=1;m<query_word_num;m++){
            int mth_short=sorted_index[m];
            POSTING_LIST searching_list=list[mth_short];
            //if the element in this part meets the tail of any list, then the elements behind it certainly can not be the intersection
            //so this thread and threads behind it can stop searching
            if(key_element>searching_list.arr[searching_list.len-1]){
                pthread_mutex_lock(&mutex);
                temp_result_list[thread_id]=result_in_thread;
                pthread_mutex_unlock(&mutex);
                for(int i=thread_id+1;i<THREAD_NUM;i++){
                    pthread_cancel(thread[i]);
                }
                pthread_exit(nullptr);
            }
            int pos=binary_search_with_position(&searching_list, key_element, finding_pointer[mth_short]);
            if(pos>=searching_list.len|| searching_list.arr[pos] != key_element){
                flag=false;
                break;
            }
            finding_pointer[mth_short]=pos;
        }
        if(flag){
            result_in_thread.push_back(key_element);
        }
    }
    pthread_mutex_lock(&mutex);
    temp_result_list[thread_id]=result_in_thread;
    pthread_mutex_unlock(&mutex);
    pthread_exit(nullptr);

}

void parallel_sAdp(POSTING_LIST *queried_posting_list, int query_word_num, vector<unsigned int> &result_list) {

    //start with sorting the posting list to find the shortest one
    int *sorted_index = new int[query_word_num];
    get_sorted_index(queried_posting_list, query_word_num, sorted_index);
// divide the elements needs to find int the first list into THREAD_NUM parts

    block_Parm threadParm[THREAD_NUM];
    int num_elements=queried_posting_list[sorted_index[0]].len;
    int elements_per_thread=num_elements/THREAD_NUM;
    int num_element_per_round;
    for(int section=0;section<THREAD_NUM;section++)
    {
        if(section==THREAD_NUM-1)
            num_element_per_round=num_elements-elements_per_thread*section;
        else
            num_element_per_round=elements_per_thread;
        threadParm[section].thread_id=section;
        threadParm[section].list=queried_posting_list;
        threadParm[section].query_word_num=query_word_num;
        threadParm[section].sorted_index=sorted_index;
        threadParm[section].finding_elements=num_element_per_round;
        threadParm[section].start_pos=elements_per_thread*section;
        pthread_create(&thread[section],nullptr,&parallel_finding_element,(void*)&threadParm[section]);
    }
    for(auto & section : thread)
    {
        pthread_join(section,nullptr);
    }
    for(int i=0;i<THREAD_NUM;i++)
    {
        result_list.insert(result_list.end(),temp_result_list[i].begin(),temp_result_list[i].end());
    }
}

void query_starter(vector<vector<unsigned int>> &simplified_Adp_result) {

    time_get_intersection.start();
    for (int i = 0; i < QueryNum; i++) {
        int query_word_num = query_list_container[i].size();
        //get the posting list of ith query
        auto *queried_posting_list = new POSTING_LIST[query_word_num];
        for (int j = 0; j < query_word_num; j++) {
            int query_list_item = query_list_container[i][j];
            queried_posting_list[j] = posting_list_container[query_list_item];
        }
        //get the result of ith query
        vector<unsigned int> simplified_Adp_result_list;
        parallel_sAdp(queried_posting_list, query_word_num, simplified_Adp_result_list);
        simplified_Adp_result.push_back(simplified_Adp_result_list);
        simplified_Adp_result_list.clear();
        delete[] queried_posting_list;
    }
    time_get_intersection.finish();
}

int main() {

    if (read_posting_list(posting_list_container) || read_query_list(query_list_container)) {
        printf("read_posting_list failed\n");
        free(posting_list_container);
        return -1;
    } else {
        printf("query_num: %d\n", QueryNum);
        vector<vector<unsigned int>> simplified_Adp_result;
        query_starter(simplified_Adp_result);
//        for (int j = 0; j < 5; ++j) {
//            printf("result %d: ", j);
//            printf("%zu\n", simplified_Adp_result[j].SIZE());
//            for (unsigned int k : simplified_Adp_result[j]) {
//                printf("%d ", k);
//            }
//            printf("\n");
//        }

        time_get_intersection.get_duration("sequential plain");
        free(posting_list_container);
        return 0;
    }
}