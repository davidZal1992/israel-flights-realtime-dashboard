package com.dzaltsman.israelflightdashboard.services;

import org.springframework.stereotype.Service;

@Service
public interface TransformationService<T> {
    public T transform(T transformationInput);
}
