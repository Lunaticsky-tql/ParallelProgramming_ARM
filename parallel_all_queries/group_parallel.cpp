#include <iostream>
#include "../readdata.h"
#include<pthread.h>

using namespace std;
POSTING_LIST *posting_list_container = (struct POSTING_LIST *) malloc(POSTING_LIST_NUM * sizeof(struct POSTING_LIST));
vector<vector<int> > query_list_container;
MyTimer time_get_intersection;

int QueryNum = 500;
const int THREAD_NUM = 128;
typedef struct {
    int threadID;
    int section;
    int query_word_num;
    POSTING_LIST *queried_posting_list;
} bundle_parm;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
vector<vector<unsigned int>> simplified_Adp_result(QueryNum);

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
    //returns the position of the element if found,
    //otherwise returns the position of the first element not smaller than it
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
                                                       finding_pointer[sorted_index[m]]);
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

void* simplified_Adp_thread(void *parm)
{

    int threadID = ((bundle_parm *)parm)->threadID;
    int section = ((bundle_parm *)parm)->section;
    int query_word_num = ((bundle_parm *)parm)->query_word_num;
    POSTING_LIST *queried_posting_list = ((bundle_parm *)parm)->queried_posting_list;
    vector<unsigned int> result_list;
    sequential(queried_posting_list, query_word_num, result_list);
    pthread_mutex_lock(&mutex);
    simplified_Adp_result[section+threadID] = result_list;
    pthread_mutex_unlock(&mutex);
    pthread_exit(nullptr);
}
    



void query_starter() {

    time_get_intersection.start();
// parting the query task into 8 parts and use 8 threads each time to do the total tasks.
    pthread_t thread[THREAD_NUM];
    bundle_parm threadParm[THREAD_NUM];
    int numThreads;
    for (int section=0;section<QueryNum;section+=THREAD_NUM)
    {
        if(section + THREAD_NUM <= QueryNum)
        {
            numThreads = THREAD_NUM;
        }else {
            numThreads = QueryNum - section;
        }
        for (int i = 0; i < numThreads; i++) {
            threadParm[i].threadID = i;
            threadParm[i].section = section;
            threadParm[i].query_word_num = query_list_container[section + i].size();
            threadParm[i].queried_posting_list = new POSTING_LIST[threadParm[i].query_word_num];
            for (int j = 0; j < threadParm[i].query_word_num; j++) {
                int query_list_item = query_list_container[section + i][j];
                threadParm[i].queried_posting_list[j] = posting_list_container[query_list_item];
            }
            pthread_create(&thread[i], nullptr, simplified_Adp_thread, (void *) &threadParm[i]);
        }
        for(int i = 0; i < numThreads; i++)
        {
            pthread_join(thread[i], nullptr);
        }
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

        query_starter();
        for (int j = 0; j < 5; ++j) {
            printf("result %d: ", j);
            printf("%zu\n", simplified_Adp_result[j].size());
            for (int k = 0; k < simplified_Adp_result[j].size(); ++k) {
                printf("%d ", simplified_Adp_result[j][k]);
            }
            printf("\n");
        }
        //parallel all queries
        time_get_intersection.get_duration("sequential paq time");
        free(posting_list_container);
        return 0;
    }

}
