#include <iostream>
#include <arm_neon.h>
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


void SvS_zip_zap(POSTING_LIST *queried_posting_list, int query_word_num, vector<unsigned int> &result_list) {
    int *sorted_index = new int[query_word_num];
    get_sorted_index(queried_posting_list, query_word_num, sorted_index);
    //Put the shortest inverted list at the front
    unsigned int *result_array;

    result_array =queried_posting_list[sorted_index[0]].arr;
    for (int i = 1; i < query_word_num; i++) {
     unsigned int *temp_array = queried_posting_list[sorted_index[i]].arr;
     unsigned int rounded_len0=(queried_posting_list[sorted_index[0]].len/4)*4;
     unsigned int rounded_leni=(queried_posting_list[sorted_index[i]].len/4)*4;
     unsigned int p0=0,pi=0;
        while(p0<rounded_len0&&pi<rounded_leni) {
           //load segments of four 32-bit elements using NEON
            uint32x4_t v0 = vld1q_u32(result_array+p0);
            uint32x4_t vi = vld1q_u32(temp_array+pi);
            //get the last element of each segment
            uint32_t v0_last = vgetq_lane_u32(v0,3);
            uint32_t vi_last = vgetq_lane_u32(vi,3);
            //move pointers
            p0+=v0_last<=vi_last?4:0;
            pi+=vi_last<=v0_last?4:0;
            //cycle shift the elements of each segment
//            _mm_shuffle_epi32(v0,_MM_SHUFFLE(3,3,3,3));
        }
//        static __m128i shuffle_mask[16]; // precomputed dictionary
//
//        size_t intersect_vector(int32 *A, int32 *B, size_t s_a, size_t s_b, int32 *C) {
//            size_t count = 0;
//            size_t i_a = 0, i_b = 0;
//
//            // trim lengths to be a multiple of 4
//            size_t st_a = (s_a / 4) * 4;
//            size_t st_b = (s_b / 4) * 4;
//
//            while(i_a < s_a && i_b < s_b) {
//                //[ load segments of four 32-bit elements
//                __m128i v_a = _mm_load_si128((__m128i*)&A[i_a]);
//                __m128i v_b = _mm_load_si128((__m128i*)&B[i_b]);
//                //]
//
//                //[ move pointers
//                int32 a_max = _mm_extract_epi32(v_a, 3);
//                int32 b_max = _mm_extract_epi32(v_b, 3);
//                i_a += (a_max <= b_max) * 4;
//                i_b += (a_max >= b_max) * 4;
//                //]
//
//                //[ compute mask of common elements
//                int32 cyclic_shift = _MM_SHUFFLE(0,3,2,1);
//                __m128i cmp_mask1 = _mm_cmpeq_epi32(v_a, v_b);    // pairwise comparison
//                v_b = _mm_shuffle_epi32(v_b, cyclic_shift);       // shuffling
//                __m128i cmp_mask2 = _mm_cmpeq_epi32(v_a, v_b);    // again...
//                v_b = _mm_shuffle_epi32(v_b, cyclic_shift);
//                __m128i cmp_mask3 = _mm_cmpeq_epi32(v_a, v_b);    // and again...
//                v_b = _mm_shuffle_epi32(v_b, cyclic_shift);
//                __m128i cmp_mask4 = _mm_cmpeq_epi32(v_a, v_b);    // and again.
//                __m128i cmp_mask = _mm_or_si128(
//                        _mm_or_si128(cmp_mask1, cmp_mask2),
//                        _mm_or_si128(cmp_mask3, cmp_mask4)
//                ); // OR-ing of comparison masks
//                // convert the 128-bit mask to the 4-bit mask
//                int32 mask = _mm_movemask_ps((__m128)cmp_mask);
//                //]
//
//                //[ copy out common elements
//                __m128i p = _mm_shuffle_epi8(v_a, shuffle_mask[mask]);
//                _mm_storeu_si128((__m128i*)&C[count], p);
//                count += _mm_popcnt_u32(mask); // a number of elements is a weight of the mask
//                //]
//            }
//
//            // intersect the tail using scalar intersection
//            ...
//
//            return count;
//        }


    }
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
        vector<vector<unsigned int>> svs_result;
            svs_zip_zap_starter(svs_result);
        //test the correctness of the result
        for (int j = 0; j < 5; ++j) {
            printf("query %d:", j);
            printf("%zu\n", svs_result[j].size());
            for (unsigned int k: svs_result[j]) {
                printf("%d ", k);
            }
            printf("\n");
        }
        time_get_intersection.get_duration("zip_zap_SIMD");
        free(posting_list_container);
        return 0;
    }
}

