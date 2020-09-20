package com.hao.walnut.log;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class LogFileConf {
    String workspace;
    String fileName;
    int maxThread = 4;
}
