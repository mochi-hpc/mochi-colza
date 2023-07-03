/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <colza/Provider.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <unistd.h>
#include <mpi.h>
#include <ssg-mpi.h>
#include <spdlog/spdlog.h>
#include <tclap/CmdLine.h>

#ifdef COLZA_ENABLE_DRC
extern "C" {
#include <rdmacred.h>
}
#endif

namespace tl = thallium;

static std::string g_address        = "na+sm";
static int         g_num_threads    = 0;
static std::string g_log_level      = "info";
static std::string g_ssg_file       = "";
static std::string g_config_file    = "";
static bool        g_join           = false;
static unsigned    g_swim_period_ms = 1000;
static int64_t     g_drc_credential = -1;

static void parse_command_line(int argc, char** argv);
static int64_t setup_credentials();
static uint32_t get_credential_cookie(int64_t credential_id);
static void update_group_file(void* group_data, ssg_member_id_t member_id, ssg_member_update_type_t update_type);

int main(int argc, char** argv) {
    parse_command_line(argc, argv);
    spdlog::set_level(spdlog::level::from_str(g_log_level));

    // Initialize MPI
    MPI_Init(&argc, &argv);
    int rank, ret;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    ssg_group_id_t gid;

    uint32_t cookie = 0;
    if(!g_join) {
        g_drc_credential = g_drc_credential == -1 ? setup_credentials() : g_drc_credential;
        spdlog::trace("Credential id is {}", g_drc_credential);
        cookie = get_credential_cookie(g_drc_credential);
    } else {
        ret = ssg_get_group_cred_from_file(g_ssg_file.c_str(), &g_drc_credential);
        if(ret != SSG_SUCCESS) {
            spdlog::critical("Could not get credentials from SSG file");
            exit(-1);
        }
        spdlog::trace("Credential id read from SSG file: {}", g_drc_credential);
        if(g_drc_credential != -1)
            cookie = get_credential_cookie(g_drc_credential);
    }

    hg_init_info hii;
    memset(&hii, 0, sizeof(hii));
    std::string cookie_str = std::to_string(cookie);
    if(g_drc_credential != -1)
        hii.na_init_info.auth_key = cookie_str.c_str();

    // we use one RPC thread to run SSG RPCs
    tl::engine engine(g_address, THALLIUM_SERVER_MODE, false, 1, &hii);
    engine.enable_remote_shutdown();

    // Initialize SSG
    ret = ssg_init();
    if(ret != SSG_SUCCESS) {
        spdlog::critical("Could not initialize SSG");
        exit(-1);
    }

    if(!g_join) {
        // Create SSG group using MPI
        ssg_group_config_t group_config = SSG_GROUP_CONFIG_INITIALIZER;
        group_config.swim_period_length_ms = g_swim_period_ms;
        group_config.swim_suspect_timeout_periods = 1000;
        group_config.swim_subgroup_member_count = 1;
        group_config.ssg_credential = g_drc_credential;
        ret = ssg_group_create_mpi(engine.get_margo_instance(),
                                   "mygroup",
                                   MPI_COMM_WORLD,
                                   &group_config,
                                   nullptr, nullptr, &gid);
        if(ret != SSG_SUCCESS) {
            spdlog::critical("Could not create SSG group from MPI");
            exit(-1);
        }
    } else {
        int num_addrs = SSG_ALL_MEMBERS;
        ret = ssg_group_id_load(g_ssg_file.c_str(), &num_addrs, &gid);
        if(ret != SSG_SUCCESS) {
            spdlog::critical("Could not load SSG group from {}", g_ssg_file);
            exit(-1);
        }
        // ssg_group_join will be called in the provider constructor
    }
    engine.push_prefinalize_callback([](){
            spdlog::trace("Finalizing SSG...");
            ssg_finalize();
            spdlog::trace("SSG finalized");
    });

    // Write SSG file
    if(rank == 0 && !g_ssg_file.empty() && !g_join) {
        int ret = ssg_group_id_store(g_ssg_file.c_str(), gid, SSG_ALL_MEMBERS);
        if(ret != SSG_SUCCESS) {
            spdlog::critical("Could not store SSG file {}", g_ssg_file);
            exit(-1);
        }
    }

    // Create Mona instance
    mona_instance_t mona = mona_init_thread(g_address.c_str(), true, &hii.na_init_info, true);

    // Print MoNA address for information
    mona_addr_t mona_addr;
    mona_addr_self(mona, &mona_addr);
    std::vector<char> mona_addr_buf(256);
    size_t mona_addr_size = 256;
    mona_addr_to_string(mona, mona_addr_buf.data(), &mona_addr_size, mona_addr);
    spdlog::debug("MoNA address is {}", mona_addr_buf.data());
    mona_addr_free(mona, mona_addr);

    // Read config file
    std::string config;
    if(!g_config_file.empty()) {
        std::ifstream t(g_config_file.c_str());
        config = std::string((std::istreambuf_iterator<char>(t)),
                              std::istreambuf_iterator<char>());
    }

    // create ESs
    std::vector<tl::managed<tl::xstream>> colza_xstreams;
    tl::managed<tl::pool> managed_colza_pool = tl::pool::create(tl::pool::access::mpmc);
    for(int i=0; i < g_num_threads; i++) {
        colza_xstreams.push_back(
            tl::xstream::create(tl::scheduler::predef::basic_wait, *managed_colza_pool));
    }
    tl::pool colza_pool;
    if(g_num_threads == 0) {
        colza_pool = *managed_colza_pool;
    } else {
        colza_pool = tl::xstream::self().get_main_pools(1)[0];
    }
    engine.push_finalize_callback([&colza_xstreams, &colza_pool](){
        spdlog::trace("Joining Colza xstreams");
        for(auto& es : colza_xstreams) {
            es->make_thread([]() {
                tl::xstream::self().exit();
            }, tl::anonymous());
        }
        usleep(1);
        for(auto& es : colza_xstreams) {
            es->join();
        }
        colza_xstreams.clear();
        spdlog::trace("Colza xstreams joined");
    });

    colza::Provider provider(engine, gid, g_join, mona, 0, config, colza_pool);

    // Add a callback to rewrite the SSG file when the group membership changes
    ssg_group_add_membership_update_callback(
            gid, update_group_file,
            reinterpret_cast<void*>(gid));

    spdlog::info("Server running at address {}", (std::string)engine.self());
    engine.wait_for_finalize();

    spdlog::trace("Engine finalized, now finalizing MoNA...");
    mona_finalize(mona);
    spdlog::trace("MoNA finalized");

    MPI_Finalize();

    return 0;
}

void parse_command_line(int argc, char** argv) {
    try {
        TCLAP::CmdLine cmd("Spawns a Colza daemon", ' ', "0.1");
        TCLAP::ValueArg<std::string> addressArg("a","address","Address or protocol (e.g. ofi+tcp)", true,"","string");
        TCLAP::ValueArg<int> numThreads("t","num-threads", "Number of threads for RPC handlers", false, 0, "int");
        TCLAP::ValueArg<std::string> logLevel("v","verbose",
                "Log level (trace, debug, info, warning, error, critical, off)", false, "info", "string");
        TCLAP::ValueArg<std::string> ssgFile("s", "ssg-file", "SSG file name", false, "", "string");
        TCLAP::ValueArg<std::string> configFile("c", "config", "config file name", false, "", "string");
        TCLAP::SwitchArg joinGroup("j","join","Join an existing group rather than create it", false);
        TCLAP::ValueArg<unsigned> swimPeriod("p","swim-period-length", "Length of the SWIM period in milliseconds", false, 1000, "int");
        TCLAP::ValueArg<int64_t> drc("d","drc-credential-id", "DRC credential ID, if already setup", false, -1, "int");
        cmd.add(addressArg);
        cmd.add(numThreads);
        cmd.add(logLevel);
        cmd.add(ssgFile);
        cmd.add(configFile);
        cmd.add(joinGroup);
        cmd.add(swimPeriod);
        cmd.add(drc);
        cmd.parse(argc, argv);
        g_address        = addressArg.getValue();
        g_num_threads    = numThreads.getValue();
        g_log_level      = logLevel.getValue();
        g_ssg_file       = ssgFile.getValue();
        g_config_file    = configFile.getValue();
        g_join           = joinGroup.getValue();
        g_swim_period_ms = swimPeriod.getValue();
        g_drc_credential = drc.getValue();
    } catch(TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
        exit(-1);
    }
}

void update_group_file(void* group_data, ssg_member_id_t, ssg_member_update_type_t) {
    ssg_group_id_t gid = reinterpret_cast<ssg_group_id_t>(group_data);
    int r = -1;
    ssg_get_group_self_rank(gid, &r);
    if(r != 0) return;
    int ret = ssg_group_id_store(g_ssg_file.c_str(), gid, SSG_ALL_MEMBERS);
    if(ret != SSG_SUCCESS) {
        spdlog::error("Could not store updated SSG file {}", g_ssg_file);
    }
}

int64_t setup_credentials() {
    uint32_t drc_credential_id = -1;
#ifdef COLZA_ENABLE_DRC
    if(g_address.find("gni") == std::string::npos)
        return -1;

    int      rank;
    int      ret;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(rank == 0) {
        ret = drc_acquire(&drc_credential_id, DRC_FLAGS_FLEX_CREDENTIAL);
        if(ret != DRC_SUCCESS) {
            spdlog::critical("drc_acquire failed (ret = {})", ret);
            exit(-1);
        }
    }

    MPI_Bcast(&drc_credential_id, 1, MPI_UINT32_T, 0, MPI_COMM_WORLD);

    if(rank == 0) {
        ret = drc_grant(drc_credential_id, drc_get_wlm_id(), DRC_FLAGS_TARGET_WLM);
        if(ret != DRC_SUCCESS) {
            spdlog::critical("drc_grant failed (ret = {})", ret);
            exit(-1);
        }
        spdlog::info("DRC credential id: {}", drc_credential_id);
    }

#endif
    return drc_credential_id;
}

uint32_t get_credential_cookie(int64_t credential_id) {
    uint32_t          drc_cookie = 0;

    if(credential_id < 0)
        return drc_cookie;
    if(g_address.find("gni") == std::string::npos)
        return drc_cookie;

#ifdef COLZA_ENABLE_DRC

    drc_info_handle_t drc_credential_info;
    int               ret;

    ret = drc_access(credential_id, 0, &drc_credential_info);
    if(ret != DRC_SUCCESS) {
        spdlog::critical("drc_access failed (ret = {})", ret);
        exit(-1);
    }

    drc_cookie = drc_get_first_cookie(drc_credential_info);

#endif
    return drc_cookie;
}
