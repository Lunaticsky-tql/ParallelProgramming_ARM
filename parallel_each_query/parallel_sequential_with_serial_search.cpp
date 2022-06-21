//
// Created by 田佳业 on 2022/6/17.
//


#include <iostream>
#include <arm_neon.h>
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
    int *start_pos;
    int *length;
} block_Parm;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
vector<vector<unsigned int>> temp_result_list(THREAD_NUM, vector<unsigned int>());

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

int binary_search_with_position_seq(POSTING_LIST *list, unsigned int element, int index, int end_pos) {
    //If found, return the position of the element; otherwise, return the position not less than its first element
    int low = index, high = end_pos, mid;
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

int serial_search_with_location_using_SIMD(POSTING_LIST *list, unsigned int element, int index, int end_pos) {
    //using NEON SIMD instructions, so we can compare 4 elements at a time
    int end_list=end_pos+1;
    int remainder=end_list%4;
    for (int i = index; i < end_list-remainder; i += 4) {
        //duplicate the element to compare with using NEON SIMD instructions
        uint32x4_t element_vec = vdupq_n_u32(element);
        uint32x4_t list_vec = vld1q_u32(list->arr + i);
        //compare the element with the list using NEON SIMD instructions
        uint32x4_t result_vec = vcgeq_u32(list_vec, element_vec);
        unsigned int* result_ptr = (unsigned int*) &result_vec;
        //if the element is not found, return the index of the first element that is not less than the element
        if(result_ptr[0]|result_ptr[1]|result_ptr[2]|result_ptr[3])
            for(int j = 0; j < 4; j++) {
                if(result_ptr[j] != 0)
                    return i + j;
            }
    }
    //look for the rest of the elements
    for (int i = end_list-remainder; i <end_list; i++) {
        if (list->arr[i] >= element)
            return i;
    }
    return end_list;
}

void *parallel_sequential(void *task) {
    int thread_id = ((block_Parm *) task)->thread_id;
    POSTING_LIST *list = ((block_Parm *) task)->list;
    int query_word_num = ((block_Parm *) task)->query_word_num;
    int *sorted_index = ((block_Parm *) task)->sorted_index;
    int *start_pos = ((block_Parm *) task)->start_pos;
    int *length = ((block_Parm *) task)->length;
    vector<unsigned int> result_in_thread;
    //sequential algorithm in the block
    bool flag;
    unsigned int key_element;
    vector<int> finding_pointer(query_word_num, 0);
    vector<int> end_pos(query_word_num, 0);
    for (int i = 0; i < query_word_num; i++) {
        finding_pointer[i] = start_pos[i];
        end_pos[i] = start_pos[i] + length[i] - 1;
    }

    key_element = list[sorted_index[0]].arr[finding_pointer[0]];
    int gaping_mth_short = 0;
    while (true) {
        flag = true;

        for (int m = 0; m < query_word_num; m++) {
            if (m == gaping_mth_short) {
                continue;
            } else {
                int mth_short = sorted_index[m];
                POSTING_LIST searching_list = list[mth_short];
                int location = serial_search_with_location_using_SIMD(&searching_list, key_element, finding_pointer[m], end_pos[m]);
                if (searching_list.arr[location] != key_element) {
                    if (location == end_pos[m] + 1) {
                        goto end_seq;
                    }
                    flag = false;
                    key_element = searching_list.arr[location];
                    finding_pointer[m] = location;
                    gaping_mth_short = m;
                    break;
                }
                finding_pointer[m] = location;
            }
        }
        if (flag) {
            //if the key element is found in all the lists, we choose the key element from the list used in last turn
            result_in_thread.push_back(key_element);
            finding_pointer[gaping_mth_short]++;
            key_element = list[sorted_index[gaping_mth_short]].arr[finding_pointer[gaping_mth_short]];
        }
        if (finding_pointer[gaping_mth_short] ==
            end_pos[gaping_mth_short] + 1) {
            //all the elements in the list are smaller than the key element, algorithm end
            goto end_seq;
        }
    }
    end_seq:
    pthread_mutex_lock(&mutex);
    temp_result_list[thread_id] = result_in_thread;
    pthread_mutex_unlock(&mutex);
    pthread_exit(nullptr);


}

void divide_task(POSTING_LIST *queried_posting_list, int query_word_num, vector<unsigned int> &result_list) {

    //get the key element from the list which just failed to find in the binary search each time
    //start with sorting the posting list to find the shortest one
    int *sorted_index = new int[query_word_num];
    get_sorted_index(queried_posting_list, query_word_num, sorted_index);

    // divide the elements needs to find int the first list into THREAD_NUM parts
    int *divide_pos_map = new int[query_word_num * (THREAD_NUM + 1)];
    for (int i = 0; i < query_word_num * THREAD_NUM; i++) {
        divide_pos_map[i] = 0;
    }
    for (int i = 0; i < query_word_num; i++) {
        divide_pos_map[query_word_num * THREAD_NUM + i] = queried_posting_list[sorted_index[i]].len;
    }
    int i;
    bool early_end = false;
    for (i = 0; i < THREAD_NUM; i++) {
        divide_pos_map[i * query_word_num] = i * (queried_posting_list[sorted_index[0]].len / THREAD_NUM);
        for (int j = 1; j < query_word_num; j++) {
            divide_pos_map[i * query_word_num + j] = binary_search_with_position(&queried_posting_list[sorted_index[j]],
                                                                                 queried_posting_list[sorted_index[0]].arr[divide_pos_map[
                                                                                         i * query_word_num]],
                                                                                 i > 0 ? divide_pos_map[
                                                                                         (i - 1) * query_word_num + j]
                                                                                       : 0);
            //early stop if the element is not found
            if (divide_pos_map[i * query_word_num + j] == queried_posting_list[sorted_index[j]].len || early_end) {
                early_end = true;
                if (j == query_word_num - 1)
                    goto early_stop;
            }
        }
    }
    early_stop:

    int thread_actually_needed = i;

    pthread_t *thread_handles;
    thread_handles = (pthread_t *) malloc(thread_actually_needed * sizeof(pthread_t));
    block_Parm threadParm[THREAD_NUM];
    int *divided_length = new int[thread_actually_needed * query_word_num]();
    for (int m = 0; m < thread_actually_needed; m++) {
        for (int j = 0; j < query_word_num; j++) {
            divided_length[m * query_word_num + j] =
                    divide_pos_map[(m + 1) * query_word_num + j] - divide_pos_map[m * query_word_num + j];
        }
    }

    for (int t = 0; t < thread_actually_needed; t++) {
        int *thread_length_vector = new int[query_word_num]();
        int *thread_index_vector = new int[query_word_num]();
        int *temp_index_vector = new int[query_word_num]();
        for (int j = 0; j < query_word_num; j++) {
            int temp = INT32_MAX;
            int temp_pos = 0;
            for (int k = 0; k < query_word_num; k++) {
                if (divided_length[t * query_word_num + k] < temp) {
                    temp = divided_length[t * query_word_num + k];
                    temp_pos = k;
                }
            }
            thread_length_vector[j] = temp;
            thread_index_vector[j] = sorted_index[temp_pos];
            temp_index_vector[j] = temp_pos;
            divided_length[t * query_word_num + temp_pos] = INT32_MAX;
        }

        threadParm[t].thread_id = t;
        threadParm[t].list = queried_posting_list;
        threadParm[t].query_word_num = query_word_num;
        threadParm[t].sorted_index = thread_index_vector;
        int *thread_first_pos = new int[query_word_num]();

        for (int j = 0; j < query_word_num; j++) {
            thread_first_pos[j] = divide_pos_map[t * query_word_num + temp_index_vector[j]];
        }
        threadParm[t].length = thread_length_vector;
        threadParm[t].start_pos = thread_first_pos;

        delete[] temp_index_vector;
        pthread_create(&thread_handles[t], nullptr, &parallel_sequential, (void *) &threadParm[t]);
    }

    for (int j = 0; j < thread_actually_needed; ++j) {
        pthread_join(thread_handles[j], nullptr);
    }
    for (int n = 0; n < thread_actually_needed; ++n) {
        result_list.insert(result_list.end(), temp_result_list[n].begin(), temp_result_list[n].end());
    }
    delete[] sorted_index;
    delete[] divide_pos_map;
    delete[] divided_length;
    free(thread_handles);

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
        divide_task(queried_posting_list, query_word_num, simplified_Adp_result_list);
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
        for (int j = 0; j < 5; ++j) {
            printf("result %d: ", j);
            printf("%zu\n", simplified_Adp_result[j].size());
            for (unsigned int k: simplified_Adp_result[j]) {
                printf("%d ", k);
            }
            printf("\n");
        }
        time_get_intersection.get_duration("sequential plain");
        free(posting_list_container);
        return 0;
    }
}
