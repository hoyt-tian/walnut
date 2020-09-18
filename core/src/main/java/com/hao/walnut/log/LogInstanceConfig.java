package com.hao.walnut.log;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogInstanceConfig {
    String workspace;
    String fileName;
    String mode = LogFileFactory.Mode_Raf;
}
