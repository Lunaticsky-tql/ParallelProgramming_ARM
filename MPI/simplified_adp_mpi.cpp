#include <iostream>
#include "../readdata.h"
#include "/usr/local/include/mpi.h"

using namespace std;
POSTING_LIST *posting_list_container = (struct POSTING_LIST *) malloc(POSTING_LIST_NUM * sizeof(struct POSTING_LIST));
vector<vector<int> > query_list_container;
MyTimer time_get_intersection;
int MY_RANK, SIZE;

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
    vector<unsigned int> temp_result_list;
    int l0_len = queried_posting_list[sorted_index[0]].len;
    int divider = l0_len / SIZE;
    int start_pos, end_pos;
    if (MY_RANK == SIZE - 1) {
        start_pos = MY_RANK * divider;
        end_pos = l0_len;
    } else {
        start_pos = MY_RANK * divider;
        end_pos = (MY_RANK + 1) * divider;
    }
    bool flag;
    unsigned int key_element= queried_posting_list[sorted_index[0]].arr[start_pos];
    vector<int> finding_pointer(query_word_num, 0);
    for (int k = start_pos; k < end_pos; k++) {
        flag= true;
        key_element = queried_posting_list[sorted_index[0]].arr[k];
        for(int m=1; m<query_word_num; m++) {
            int mth_short = sorted_index[m];
            POSTING_LIST searching_list = queried_posting_list[mth_short];
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
        if(flag) {
            temp_result_list.push_back(key_element);
        }
    }
    end: delete[] sorted_index;
    if(MY_RANK == 0) {
        //put the temp_result_list to the result_list
        result_list.assign(temp_result_list.begin(), temp_result_list.end());
        for(int i=1; i<SIZE; i++) {
            int size = 0;
            MPI_Recv(&size, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            temp_result_list.clear();
            temp_result_list.resize(size);
            MPI_Recv(&temp_result_list[0], size, MPI_UNSIGNED, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            result_list.insert(result_list.end(), temp_result_list.begin(), temp_result_list.end());
        }
    }
    else {
        //send size of temp_result_list to the root
        int temp_result_list_size = temp_result_list.size();
        MPI_Send(&temp_result_list_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        //only send array rather than whole struct
        MPI_Send(&temp_result_list[0], temp_result_list.size(), MPI_UNSIGNED, 0, 0, MPI_COMM_WORLD);
    }
}

void query_starter(vector<vector<unsigned int> > &simplified_Adp_result) {

    MPI_Comm_rank(MPI_COMM_WORLD, &MY_RANK);
    MPI_Comm_size(MPI_COMM_WORLD, &SIZE);
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

int main() {

    if (read_posting_list(posting_list_container) || read_query_list(query_list_container)) {
        printf("read_posting_list failed\n");
        free(posting_list_container);
        return -1;
    } else {
        printf("query_num: %d\n", QueryNum);
        MPI_Init(NULL, NULL);
        vector<vector<unsigned int> > simplified_Adp_result;
        query_starter(simplified_Adp_result);
        if(MY_RANK == 0) {
            for (int j = 0; j < 5; ++j) {
                printf("result %d: ", j);
                printf("%zu\n", simplified_Adp_result[j].size());
                for (int k = 0; k < simplified_Adp_result[j].size(); ++k) {
                    printf("%d ", simplified_Adp_result[j][k]);
                }
                printf("\n");
            }
            time_get_intersection.get_duration("simplified_Adp plain");
        }

        free(posting_list_container);
        MPI_Finalize();
        return 0;
    }
}
