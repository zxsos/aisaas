package io.bangbang.commons.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CacheObject implements Serializable {

    @Serial
    private static final long serialVersionUID = 2852420665773757754L;

    private Object object; // NOSONAR
}
