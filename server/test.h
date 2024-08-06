#ifndef TEST_H_
#define TEST_H_

#include <assert.h>
#include <stdio.h>

#define RUN_TEST(name)       \
  do {                       \
    printf("%s... ", #name); \
    fflush(stdout);          \
    name();                  \
    printf("PASS\n");        \
  } while (false)

#endif
