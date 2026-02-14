#pragma once

#include <string>
#include <iostream>
#include <cstdint>

struct DBConfigs {
    u_int16_t page_size = 4096; 
    std::string db_file_name = "data.db";
};

class ConfigManager {
public:
    static DBConfigs LoadConfig(const std::string& path) {
        DBConfigs config;
        config.page_size = 4096; 
        config.db_file_name = "my_database.db";
        return config;
    }
};