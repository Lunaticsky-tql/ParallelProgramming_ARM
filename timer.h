//
// Created by 田佳业 on 2022/4/27.
//

#include<sys/time.h>
using namespace std;
#ifndef PTHREAD_TIMER_H
#define PTHREAD_TIMER_H

#endif //PTHREAD_TIMER_H

class MyTimer {
private:
    struct timeval start_;
    struct timeval end;
    double timeMS;

public:
    MyTimer() {
        timeMS = 0.0;
    }


    void start() {
        gettimeofday(&start_, NULL);
    }

    void finish() {
        gettimeofday(&end, NULL);
    }

    void get_duration(const string& str) {
        timeMS = (end.tv_sec - start_.tv_sec) * 1000.0;
        timeMS += (end.tv_usec - start_.tv_usec) / 1000.0;
        printf("%s: %.3f ms\n", str.c_str(), timeMS);
    }

    };