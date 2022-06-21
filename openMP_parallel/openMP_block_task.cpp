#include <iostream>
#include "../readdata.h"

using namespace std;
POSTING_LIST *posting_list_container = (struct POSTING_LIST *) malloc(POSTING_LIST_NUM * sizeof(struct POSTING_LIST));
vector<vector<int>> query_list_container;
MyTimer time_get_intersection;
static int search_time = 0;
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

int binary_search_with_position(POSTING_LIST *list, unsigned int element, int index) {
    search_time++;
    //如果找到返回该元素位置，否则返回不小于它的第一个元素的位置
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

int serial_search_with_location(POSTING_LIST *list, unsigned int element, int index) {
    while(index < list->len) {
        if (list->arr[index] >= element)
            return index;
        else
            index++;
    }
    return index;
}

void simplified_Adp(POSTING_LIST *queried_posting_list, int query_word_num, vector<unsigned int> &result_list) {

    //start with sorting the posting list to find the shortest one
    int *sorted_index = new int[query_word_num];
    get_sorted_index(queried_posting_list, query_word_num, sorted_index);
    bool flag;
    unsigned int key_element;
    vector<int> finding_pointer(query_word_num, 0);
    for (int k = 0; k < queried_posting_list[sorted_index[0]].len; k++) {
        flag = true;
        key_element = queried_posting_list[sorted_index[0]].arr[k];
        for (int m = 1; m < query_word_num; m++) {
            int mth_short = sorted_index[m];
            POSTING_LIST searching_list = queried_posting_list[mth_short];
            //if the key element is larger than the end element of a list ,it means any element larger than the key element can not be the intersection
            if (key_element > searching_list.arr[searching_list.len - 1]) {
                goto end;
            }
//            int location = binary_search_with_position(&queried_posting_list[mth_short], key_element,
//                                                       finding_pointer[mth_short]);
            int location = serial_search_with_location(&queried_posting_list[mth_short], key_element,
                                                       finding_pointer[mth_short]);
            if (searching_list.arr[location] != key_element) {
                flag = false;
                break;
            }
            finding_pointer[mth_short] = location;
        }
        if (flag) {
            result_list.push_back(key_element);
        }
    }
    end: delete[] sorted_index;
}

void query_starter(vector<vector<unsigned int>> &simplified_Adp_result) {

    time_get_intersection.start();
    int query_word_num;
    POSTING_LIST *queried_posting_list;
    int query_list_item;
    vector<unsigned int> simplified_Adp_result_list;
#pragma omp parallel for num_threads(16) private(query_word_num,queried_posting_list,query_list_item,simplified_Adp_result_list) shared(query_list_container,simplified_Adp_result,QueryNum,posting_list_container) default(none)
    for (int i = 0; i < QueryNum; i++) {
        query_word_num = query_list_container[i].size();
        //get the posting list of ith query
        queried_posting_list = new POSTING_LIST[query_word_num];
        for (int j = 0; j < query_word_num; j++) {
            query_list_item = query_list_container[i][j];
            queried_posting_list[j] = posting_list_container[query_list_item];
        }
        simplified_Adp(queried_posting_list, query_word_num, simplified_Adp_result_list);
#pragma omp critical
        {
            simplified_Adp_result[i]=simplified_Adp_result_list;
        }
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
        vector<vector<unsigned int>> simplified_Adp_result(QueryNum, vector<unsigned int>());
        query_starter(simplified_Adp_result);
        for (int i = 0; i < 5; i++) {
            printf("query %d: %zu\n", i,simplified_Adp_result[i].size());
            for (unsigned int j : simplified_Adp_result[i]) {
                printf("%d ", j);
            }
            printf("\n");
        }
        time_get_intersection.get_duration("simplified_Adp plain");

        free(posting_list_container);
        return 0;
    }
}
