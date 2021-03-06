/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __COLZA_ERROR_CODES_HPP
#define __COLZA_ERROR_CODES_HPP

#include <cstdint>

namespace colza {

enum class ErrorCode : int32_t {
    SUCCESS                 =  0,
    PIPELINE_IS_ACTIVE      = -1,
    PIPELINE_NOT_ACTIVE     = -2,
    INVALID_ITERATION       = -3,
    INVALID_PIPELINE_NAME   = -4,
    JSON_PARSE_ERROR        = -5,
    JSON_CONFIG_ERROR       = -6,
    INVALID_SECURITY_TOKEN  = -7,
    INVALID_LIBRARY         = -8,
    SSG_ERROR               = -9,
    INVALID_INSTANCE        = -10,
    EMPTY_DIST_PIPELINE     = -11,
    MONA_ERROR              = -12,
    PIPELINE_CREATE_ERROR   = -13,
    INVALID_GROUP_HASH      = -14,
    OTHER_ERROR             = -255
};

}

#endif
