/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "colza/Client.hpp"
#include "colza/Provider.hpp"
#include "colza/ProviderHandle.hpp"
#include <bedrock/AbstractServiceFactory.hpp>

namespace tl = thallium;

class ColzaFactory : public bedrock::AbstractServiceFactory {

    public:

    ColzaFactory() {}

    void *registerProvider(const bedrock::FactoryArgs &args) override {
        margo_instance_id mid     = args.mid;
        uint16_t provider_id      = args.provider_id;
        const std::string& config = args.config;
        tl::pool pool(args.pool);
        auto it = args.dependencies.find("group");
        if(it == args.dependencies.end()) {
            // this should not happen if Bedrock does its
            // job resolving dependencies corrrectly.
            return nullptr;
        }
        ssg_group_id_t gid = reinterpret_cast<ssg_group_id_t>(
                it->second.dependencies[0]->getHandle<ssg_group_id_t>());
        it = args.dependencies.find("mona_instance");
        if(it == args.dependencies.end()) {
            // this should not happen if Bedrock does its
            // job resolving dependencies corrrectly.
            return nullptr;
        }
        mona_instance_t mona = reinterpret_cast<mona_instance_t>(
                it->second.dependencies[0]->getHandle<mona_instance_t>());
        // TODO properly handle "must_join" argument
        auto provider = new colza::Provider(mid, gid, false, mona, provider_id, config, pool);
        return static_cast<void *>(provider);
    }

    void deregisterProvider(void *p) override {
        auto provider = static_cast<colza::Provider *>(p);
        delete provider;
    }

    std::string getProviderConfig(void *p) override {
        auto provider = static_cast<colza::Provider *>(p);
        return provider->getConfig();
    }

    void *initClient(const bedrock::FactoryArgs &args) override {
        return static_cast<void *>(new colza::Client(args.mid));
    }

    void finalizeClient(void *client) override {
        delete static_cast<colza::Client *>(client);
    }

    std::string getClientConfig(void *p) override {
        (void)p;
        return "{}";
    }

    void *createProviderHandle(void *c, hg_addr_t address,
            uint16_t provider_id) override {
        auto client = static_cast<colza::Client *>(c);
        auto ph = new colza::ProviderHandle(
                client->engine(),
                address,
                provider_id,
                false);
        return static_cast<void *>(ph);
    }

    void destroyProviderHandle(void *providerHandle) override {
        auto ph = static_cast<colza::ProviderHandle *>(providerHandle);
        delete ph;
    }

    const std::vector<bedrock::Dependency> &getProviderDependencies() override {
        static std::vector<bedrock::Dependency> dependencies;
        if(dependencies.size() == 0) {
            dependencies.resize(2);
            dependencies[0].name  = "group";
            dependencies[0].type  = "ssg";
            dependencies[0].flags = BEDROCK_REQUIRED;
            dependencies[1].name  = "mona_instance";
            dependencies[1].type  = "mona";
            dependencies[1].flags = BEDROCK_REQUIRED;
        }
        return dependencies;
    }

    const std::vector<bedrock::Dependency> &getClientDependencies() override {
        static std::vector<bedrock::Dependency> dependencies;
        return dependencies;
    }
};

BEDROCK_REGISTER_MODULE_FACTORY(colza, ColzaFactory)
