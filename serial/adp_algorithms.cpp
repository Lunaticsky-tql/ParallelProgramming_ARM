#include <iostream>
#include "../readdata.h"

using namespace std;
POSTING_LIST *posting_list_container = (struct POSTING_LIST *) malloc(POSTING_LIST_NUM * sizeof(struct POSTING_LIST));
vector<vector<int>> query_list_container;
MyTimer time_get_intersection;
static int search_time = 0;
int QueryNum = 999;
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

void get_sorted_index_Adp( POSTING_LIST *queried_posting_list, int query_word_num, int *sorted_index, vector<int> &finding_pointer) {
    //Overloads for the Adp algorithm. Sorted by the number of elements that need to be queried
    for (int i = 0; i < query_word_num; i++) {
        sorted_index[i] = i;
    }
    for (int i = 0; i < query_word_num - 1; i++) {
        for (int j = i + 1; j < query_word_num; j++) {
            if (queried_posting_list[sorted_index[i]].len-finding_pointer[sorted_index[i]] > queried_posting_list[sorted_index[j]].len-finding_pointer[sorted_index[j]]) {
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
            int location = binary_search_with_position(&queried_posting_list[mth_short], key_element,
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

void Adp(POSTING_LIST *queried_posting_list, int query_word_num, vector<unsigned int> &result_list) {
    //Each time a mismatch is found during the search, it is reordered by the number of elements remaining in each table
    vector<int> finding_pointer(query_word_num, 0);
    int *sorted_index = new int[query_word_num];
    unsigned int key_element;
    //Initialization of the length record table
    for (int i = 0; i < query_word_num; i++) {
        sorted_index[i] = i;
    }
    bool flag;
    while(true) {
        get_sorted_index_Adp(queried_posting_list, query_word_num, sorted_index, finding_pointer);
        key_element = queried_posting_list[sorted_index[0]].arr[finding_pointer[sorted_index[0]]];
        flag = true;
        for (int m = 1; m < query_word_num; ++m) {
            int mth_short = sorted_index[m];
            POSTING_LIST searching_list = queried_posting_list[mth_short];
            int location = binary_search_with_position(&queried_posting_list[mth_short], key_element,
                                                       finding_pointer[sorted_index[m]]);
            if (searching_list.arr[location] != key_element) {
                if (searching_list.len==location) {
                    //If the last element is not equal to the key element, then the table is finished and the algorithm terminates
                    goto end_Adp;
                }
                flag = false;
                break;
            }
            finding_pointer[mth_short] = location;
        }
        if (flag) {
            result_list.push_back(key_element);
        }
        finding_pointer[sorted_index[0]]++;
        if(finding_pointer[sorted_index[0]]==queried_posting_list[sorted_index[0]].len)
        {
            //If the search for the current shortest inverted linked table is finished, the algorithm also stops
            goto end_Adp;
        }
    }
    end_Adp:delete[] sorted_index;
}

void simplified_adp_starter(vector<vector<unsigned int>> &simplified_Adp_result) {

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
        simplified_Adp(queried_posting_list, query_word_num, simplified_Adp_result_list);
        simplified_Adp_result.push_back(simplified_Adp_result_list);
        simplified_Adp_result_list.clear();
        delete[] queried_posting_list;
    }
    time_get_intersection.finish();
}

void classic_adp_starter(vector<vector<unsigned int>> &simplified_Adp_result) {
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
        Adp(queried_posting_list, query_word_num, simplified_Adp_result_list);
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
        printf("read_posting_list success!\n");
        printf("query_num: %d\n", QueryNum);
        vector<vector<unsigned int>> simplified_Adp_result;
        printf("1:simplified_adp is the simplified algorithm, which only choose elements in the shortest table\n");
        printf("2:adp is the classic adaptive algorithm\n");
        printf("enter the number ahead to trigger according algorithm\n");
        cin >> algorithm_num;
        switch (algorithm_num) {
            case 1:
                simplified_adp_starter(simplified_Adp_result);
                break;
            case 2:
                classic_adp_starter(simplified_Adp_result);
                break;
            default:
                printf("wrong algorithm number!\n");
                break;
        }
        //test the correctness of the result
        for (int j = 0; j < 5; ++j) {
            printf("query %d:", j);
            printf("%zu\n", simplified_Adp_result[j].size());
            for(unsigned int k : simplified_Adp_result[j]){
                printf("%d ", k);
            }
            printf("\n");
        }
        if(algorithm_num==1) {
            time_get_intersection.get_duration("simplified_adp");
        }
        else if(algorithm_num==2) {
            time_get_intersection.get_duration("adp");
        }
        free(posting_list_container);
        return 0;
    }
}
