#include <iostream>
#include "../readdata.h"

using namespace std;
POSTING_LIST *posting_list_container = (struct POSTING_LIST *) malloc(POSTING_LIST_NUM * sizeof(struct POSTING_LIST));
vector<vector<int>> query_list_container;
MyTimer time_get_intersection;
static int search_time = 0;
int QueryNum = 500;
int algorithm_num;

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

int binary_search(POSTING_LIST *list, unsigned int element, int index) {
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
    return -1;

}

int binary_search_with_position(POSTING_LIST *list, unsigned int element, int index) {
    search_time++;
    //If found, return the position of the element; otherwise, return the position of the first element that is not less than it
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

void SvS(POSTING_LIST *queried_posting_list, int query_word_num, vector<unsigned int> &result_list) {
    //intersect two shortest tables, and then intersect the results with other tables
    //First, sort the length of the inverted linked list. Since there are only a few keywords in the query, we can use the common selection sort
    //At the same time, the sorting process is also a part of the algorithm, which also needs to take into account the time
    int *sorted_index = new int[query_word_num];
    get_sorted_index(queried_posting_list, query_word_num, sorted_index);
    //Put the shortest inverted list at the front
    for (int k = 0; k < queried_posting_list[sorted_index[0]].len; k++) {
        result_list.push_back(queried_posting_list[sorted_index[0]].arr[k]);
    }
    for (int i = 1; i < query_word_num; i++) {
        vector<unsigned int> temp_result_list;

        for (int j = 0; j < result_list.size(); j++) {
            search_time++;
            if (binary_search(&queried_posting_list[sorted_index[i]], result_list[j], 0) != -1) {
                temp_result_list.emplace_back(result_list[j]);
            }
        }
        result_list = temp_result_list;
    }

}


void SvS_refine(POSTING_LIST *queried_posting_list, int query_word_num, vector<unsigned int> &result_list) {

    int *sorted_index = new int[query_word_num];
    //Sort the inverted list by the length
    get_sorted_index(queried_posting_list, query_word_num, sorted_index);
    //Record the position of the element every time it is found
    vector<int> finding_pointer(query_word_num, 0);
    //Put the shortest inverted list at the front
    for (int i = 0; i < queried_posting_list[sorted_index[0]].len; i++) {
        result_list.push_back(queried_posting_list[sorted_index[0]].arr[i]);
    }
    for (int i = 1; i < query_word_num; i++) {
        vector<unsigned int> temp_result_list;
        for (unsigned int &j: result_list) {
            int location = binary_search_with_position(&queried_posting_list[sorted_index[i]], j, finding_pointer[i]);
            if (queried_posting_list[sorted_index[i]].arr[location] == j) {
                temp_result_list.push_back(j);
            }
            //Update the finding pointer
            finding_pointer[sorted_index[i]] = location;
        }
        //Update the result list
        result_list = temp_result_list;
    }
    delete[] sorted_index;
}

void SvS_zip_zap(POSTING_LIST *queried_posting_list, int query_word_num, vector<unsigned int> &result_list) {
    int *sorted_index = new int[query_word_num];
    get_sorted_index(queried_posting_list, query_word_num, sorted_index);
    //Put the shortest inverted list at the front
    for (int k = 0; k < queried_posting_list[sorted_index[0]].len; k++) {
        result_list.push_back(queried_posting_list[sorted_index[0]].arr[k]);
    }
    for (int i = 1; i < query_word_num; i++) {
        vector<unsigned int> temp_result_list;
        unsigned int p0 = 0;
        unsigned int pi = 0;
        while (p0 < result_list.size() && pi < queried_posting_list[sorted_index[i]].len) {
            if (result_list[p0] == queried_posting_list[sorted_index[i]].arr[pi]) {
                temp_result_list.push_back(result_list[p0]);
                p0++;
                pi++;
            } else if (result_list[p0] < queried_posting_list[sorted_index[i]].arr[pi]) {
                p0++;
            } else {
                pi++;
            }
        }
        result_list = temp_result_list;
    }
}

void svs_refined_starter(vector<vector<unsigned int>> &svs_result) {

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
        vector<unsigned int> svs_result_list;
        SvS_refine(queried_posting_list, query_word_num, svs_result_list);
        svs_result.push_back(svs_result_list);
        svs_result_list.clear();
        delete[] queried_posting_list;
    }
    time_get_intersection.finish();
}

void svs_plain_starter(vector<vector<unsigned int>> &svs_result) {

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
        vector<unsigned int> svs_result_list;
        SvS(queried_posting_list, query_word_num, svs_result_list);
        svs_result.push_back(svs_result_list);
        svs_result_list.clear();
        delete[] queried_posting_list;
    }
    time_get_intersection.finish();
}


void svs_zip_zap_starter(vector<vector<unsigned int>> &svs_result) {
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
        vector<unsigned int> svs_result_list;
        SvS_zip_zap(queried_posting_list, query_word_num, svs_result_list);
        svs_result.push_back(svs_result_list);
        svs_result_list.clear();
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
        printf("read_posting_list success!\n");
        printf("query_num: %d\n", QueryNum);
        printf("1:svs_plain is the basic algorithm\n");
        printf("2:svs_refined is the refinement algorithm, which recorded the position when finding elements\n");
        printf("3:svs_zip_zap is the algorithm which uses two pointers to get intersection\n");
        printf("enter the number ahead to trigger according algorithm\n");
        cin >> algorithm_num;
        vector<vector<unsigned int>> svs_result;
        if (algorithm_num == 1) {
            svs_plain_starter(svs_result);
        } else if (algorithm_num == 2) {
            svs_refined_starter(svs_result);
        } else if (algorithm_num == 3) {
            svs_zip_zap_starter(svs_result);
        } else {
            printf("wrong algorithm number\n");
            return -1;
        }
        //test the correctness of the result
        for (int j = 0; j < 5; ++j) {
            printf("query %d:", j);
            printf("%zu\n", svs_result[j].size());
            for (unsigned int k: svs_result[j]) {
                printf("%d ", k);
            }
            printf("\n");
        }
        if (algorithm_num == 1) {
            time_get_intersection.get_duration("svs plain");
        } else if (algorithm_num == 2) {
            time_get_intersection.get_duration("svs refined");
        } else if (algorithm_num == 3) {
            time_get_intersection.get_duration("svs zip zap");
        }
        free(posting_list_container);
        return 0;
    }
}
