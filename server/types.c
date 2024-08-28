#include "types.h"

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

string string_create(size_t size) {
  if (size <= SMALL_STRING_MAX_SIZE) {
    return (string){.small = {.is_small = true, .size = size}};
  }
  uint8_t *data = malloc(size);
  assert(data != NULL);
  return (string){.heap = {.is_small = false, .size = size, .data = data}};
}

void string_destroy(string *str) {
  if (!str->is_small) {
    free(str->heap.data);
  }
}
