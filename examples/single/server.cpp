/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <colza/Provider.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <spdlog/spdlog.h>
#include <tclap/CmdLine.h>

namespace tl = thallium;

static std::string g_address     = "na+sm";
static int         g_num_threads = 0;
static std::string g_log_level   = "info";
static std::string g_ssg_file    = "";
static std::string g_config_file = "";

static void parse_command_line(int argc, char** argv);

int main(int argc, char** argv) {
    parse_command_line(argc, argv);
    spdlog::set_level(spdlog::level::from_str(g_log_level));

    // Initialize SSG
    int ret = ssg_init();
    if(ret != SSG_SUCCESS) {
        std::cerr << "Could not initialize SSG" << std::endl;
    }

    tl::engine engine(g_address, THALLIUM_SERVER_MODE, false, g_num_threads);
    engine.enable_remote_shutdown();

    // Create SSG group
    auto self_addr = static_cast<std::string>(engine.self());
    std::vector<const char*> group_addr_str = { self_addr.c_str() };
    ssg_group_config_t group_config = SSG_GROUP_CONFIG_INITIALIZER;
    ssg_group_id_t gid = SSG_GROUP_ID_INVALID;
    ssg_group_create(engine.get_margo_instance(),
                     "mygroup",
                     group_addr_str.data(),
                     1, &group_config,
                     nullptr, nullptr,
                     &gid);
    engine.push_prefinalize_callback([](){ ssg_finalize(); });

    // Write SSG file
    if(!g_ssg_file.empty()) {
        int ret = ssg_group_id_store(g_ssg_file.c_str(), gid, SSG_ALL_MEMBERS);
        if(ret != SSG_SUCCESS) {
            spdlog::critical("Could not store SSG file {}", g_ssg_file);
            exit(-1);
        }
    }

    // Create Mona instance
    mona_instance_t mona = mona_init_thread(g_address.c_str(), true, NULL, true);

    // Print MoNA address for information
    mona_addr_t mona_addr;
    mona_addr_self(mona, &mona_addr);
    std::vector<char> mona_addr_buf(256);
    size_t mona_addr_size = 256;
    mona_addr_to_string(mona, mona_addr_buf.data(), &mona_addr_size, mona_addr);
    spdlog::debug("MoNA address is {}", mona_addr_buf.data());
    mona_addr_free(mona, mona_addr);

    // Read config file if there is one
    std::string config;
    if(!g_config_file.empty()) {
        std::ifstream t(g_config_file.c_str());
        config = std::string((std::istreambuf_iterator<char>(t)),
                              std::istreambuf_iterator<char>());
    }

    colza::Provider provider(engine, gid, false, mona, 0, config);

    spdlog::info("Server running at address {}", (std::string)engine.self());
    engine.wait_for_finalize();

    mona_finalize(mona);

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
        cmd.add(addressArg);
        cmd.add(numThreads);
        cmd.add(logLevel);
        cmd.add(ssgFile);
        cmd.add(configFile);
        cmd.parse(argc, argv);
        g_address     = addressArg.getValue();
        g_num_threads = numThreads.getValue();
        g_log_level   = logLevel.getValue();
        g_ssg_file    = ssgFile.getValue();
        g_config_file = configFile.getValue();
    } catch(TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
        exit(-1);
    }
}
