//
// Created by 田佳业 on 2022/4/30.
//
#include <iostream>
#include "../readdata.h"

using namespace std;
POSTING_LIST *posting_list_container = (struct POSTING_LIST *) malloc(POSTING_LIST_NUM * sizeof(struct POSTING_LIST));
vector<vector<int> > query_list_container;
MyTimer time_get_intersection;

int QueryNum = 500;
static int search_time = 0;

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
    search_time++;
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

void sequential(POSTING_LIST *queried_posting_list, int query_word_num, vector<unsigned int> &result_list) {

    //get the key element from the list which just failed to find in the binary search each time
    //start with sorting the posting list to find the shortest one
    int *sorted_index = new int[query_word_num];
    get_sorted_index(queried_posting_list, query_word_num, sorted_index);
    bool flag;
    unsigned int key_element;
    vector<int> finding_pointer(query_word_num, 0);
    key_element = queried_posting_list[sorted_index[0]].arr[finding_pointer[sorted_index[0]]];
    //we definitely need not search the key element in the list which key element is chosen
    int gaping_mth_short = 0;
    while (true) {
        flag = true;
        for (int m = 0; m < query_word_num; ++m) {
            if (m == gaping_mth_short)
                continue;
            else {
                int mth_short = sorted_index[m];
                POSTING_LIST searching_list = queried_posting_list[mth_short];
                int location = binary_search_with_position(&queried_posting_list[mth_short], key_element,
                                                           finding_pointer[sorted_index[m]]);
                if (searching_list.arr[location] != key_element) {
                    if (searching_list.len == location) {
                        //all the elements in the list are smaller than the key element, algorithm end
                        goto end_Seq;
                    }
                    flag = false;
                    //update the key element and the pointer
                    //the location indicates the first element larger than the key element
                    key_element = searching_list.arr[location];
                    finding_pointer[mth_short] = location;
                    gaping_mth_short = m;
                    break;
                }
                finding_pointer[mth_short] = location;
            }
        }
        if (flag) {
            //if the key element is found in all the lists, we choose the key element from the list used in last turn
            result_list.push_back(key_element);
            finding_pointer[sorted_index[gaping_mth_short]]++;
            key_element = queried_posting_list[sorted_index[gaping_mth_short]].arr[finding_pointer[sorted_index[gaping_mth_short]]];
        }

        if (finding_pointer[sorted_index[gaping_mth_short]] ==
            queried_posting_list[sorted_index[gaping_mth_short]].len) {
            //all the elements in the list are smaller than the key element, algorithm end
            goto end_Seq;
        }
    }
    end_Seq:
    delete[] sorted_index;
}

void query_starter(vector<vector<unsigned int>> &Seq_result) {

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
        vector<unsigned int> Seq_result_list;
        sequential(queried_posting_list, query_word_num, Seq_result_list);
        Seq_result.push_back(Seq_result_list);
        Seq_result_list.clear();
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
        vector<vector<unsigned int>> Seq_result;
        query_starter(Seq_result);
        //test the correctness of the result
        for (int j = 0; j < 5; ++j) {
            printf("result %d: ", j);
            printf("%zu\n", Seq_result[j].size());
            for (int k = 0; k < Seq_result[j].size(); ++k) {
                printf("%d ", Seq_result[j][k]);
            }
            printf("\n");
        }
        printf("searching_time:%d\n", search_time);
        time_get_intersection.get_duration("sequential plain");
        free(posting_list_container);
        return 0;
    }
}

