package com.learn.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class ElasticsearchController {

    @Autowired
    private ElasticsearchService elasticsearchService;

    @PostMapping("/index")
    public ResponseEntity<String> indexDocument(
            @RequestParam String index,
            @RequestParam String id,
            @RequestBody Map<String, Object> document) {
        try {
            String documentId = elasticsearchService.indexDocument(index, id, document);
            return ResponseEntity.ok("Document indexed with ID: " + documentId);
        } catch (IOException e) {
            return ResponseEntity.status(500).body("Error indexing document: " + e.getMessage());
        }
    }
}
