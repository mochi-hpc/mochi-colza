/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "colza/Provider.hpp"

#include "ProviderImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace colza {

Provider::Provider(const tl::engine& engine, ssg_group_id_t gid, bool must_join, mona_instance_t mona,
                   uint16_t provider_id, const std::string& config, const tl::pool& p)
: self(std::make_shared<ProviderImpl>(engine, gid, must_join, mona, provider_id, p)) {
    self->get_engine().push_finalize_callback(this, [p=this]() { p->self.reset(); });
    self->get_engine().push_prefinalize_callback(this, [p=this]() { p->self->onPreFinalize(); });
    self->processConfig(config);
}

Provider::Provider(margo_instance_id mid, ssg_group_id_t gid, bool must_join, mona_instance_t mona,
                   uint16_t provider_id, const std::string& config, const tl::pool& p)
: self(std::make_shared<ProviderImpl>(mid, gid, must_join, mona, provider_id, p)) {
    self->get_engine().push_finalize_callback(this, [p=this]() { p->self.reset(); });
    self->get_engine().push_prefinalize_callback(this, [p=this]() { p->self->onPreFinalize(); });
    self->processConfig(config);
}

Provider::Provider(Provider&& other) {
    if(!other.self) return;
    other.self->get_engine().pop_finalize_callback(this);
    other.self->get_engine().pop_prefinalize_callback(this);
    self = std::move(other.self);
    self->get_engine().push_finalize_callback(this, [p=this]() { p->self.reset(); });
    self->get_engine().push_prefinalize_callback(this, [p=this]() { p->self->onPreFinalize(); });
}

Provider::~Provider() {
    if(self) {
        self->get_engine().pop_finalize_callback(this);
        self->get_engine().pop_prefinalize_callback(this);
    }
    spdlog::trace("Colza provider destroyed");
}

std::string Provider::getConfig() const {
    return "{}";
}

Provider::operator bool() const {
    return static_cast<bool>(self);
}

void Provider::setSecurityToken(const std::string& token) {
    if(self) self->m_token = token;
}

}
