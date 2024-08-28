#include "types.h"

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

string string_create(size_t size) {
  if (size <= SMALL_STRING_MAX_SIZE) {
    return (string){.small.size = (size << 1) | 1};
  }
  uint8_t *data = malloc(size);
  assert(data != NULL);
  return (string){.heap = {.size = size << 1, .data = data}};
}

void string_destroy(string *str) {
  if (!string_is_small(str)) {
    free(str->heap.data);
  }
}
