/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.k8s;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kubernetes.client.openapi.models.V1Job;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.HashMap;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class K8SClientTest
{
    private K8SClient k8sClient;
    private ObjectMapper objectMapper;
    private static final String TEST_JOB_JSON = """
            {
                  "apiVersion": null,
                  "kind": null,
                  "metadata": {
                      "annotations": {
                          "batch.kubernetes.io/cronjob-scheduled-timestamp": "2025-04-10T20:10:00Z"
                      },
                      "creationTimestamp": 1744315801,
                      "deletionGracePeriodSeconds": null,
                      "deletionTimestamp": null,
                      "finalizers": null,
                      "generateName": null,
                      "generation": 1,
                      "labels": {
                          "app": "vis-intel-scraper-et",
                          "chart": "cron-application-1.0.0",
                          "docketAssetID": "service__visibility-intelligence-jobs",
                          "heritage": "Tiller",
                          "release": "vis-intel-scraper-et-dvi3o0ai38lwgey3gwobsr0qv"
                      },
                      "managedFields": [
                          {
                              "apiVersion": "batch/v1",
                              "fieldsType": "FieldsV1",
                              "fieldsV1": {
                                  "f:metadata": {
                                      "f:annotations": {
                                          ".": {},
                                          "f:batch.kubernetes.io/cronjob-scheduled-timestamp": {}
                                      },
                                      "f:labels": {
                                          ".": {},
                                          "f:app": {},
                                          "f:chart": {},
                                          "f:heritage": {},
                                          "f:release": {}
                                      },
                                      "f:ownerReferences": {
                                          ".": {},
                                          "k:{\\"uid\\":\\"1d0a33cc-b564-47f6-abbb-743a7bcd5aae\\"}": {}
                                      }
                                  },
                                  "f:spec": {
                                      "f:activeDeadlineSeconds": {},
                                      "f:backoffLimit": {},
                                      "f:completionMode": {},
                                      "f:completions": {},
                                      "f:manualSelector": {},
                                      "f:parallelism": {},
                                      "f:podReplacementPolicy": {},
                                      "f:suspend": {},
                                      "f:template": {
                                          "f:metadata": {
                                              "f:annotations": {
                                                  ".": {},
                                                  "f:appVersion": {},
                                                  "f:cluster-autoscaler.kubernetes.io/safe-to-evict": {},
                                                  "f:prometheus.io/port": {},
                                                  "f:prometheus.io/scrape": {}
                                              },
                                              "f:labels": {
                                                  ".": {},
                                                  "f:app": {},
                                                  "f:chart": {},
                                                  "f:heritage": {},
                                                  "f:istio-locality": {},
                                                  "f:release": {}
                                              }
                                          },
                                          "f:spec": {
                                              "f:containers": {
                                                  "k:{\\"name\\":\\"vis-intel-scraper-et\\"}": {
                                                      ".": {},
                                                      "f:env": {
                                                          ".": {},
                                                          "k:{\\"name\\":\\"APP_NAME\\"}": {
                                                              ".": {},
                                                              "f:name": {},
                                                              "f:value": {}
                                                          },
                                                          "k:{\\"name\\":\\"APP_REGION\\"}": {
                                                              ".": {},
                                                              "f:name": {},
                                                              "f:value": {}
                                                          },
                                                          "k:{\\"name\\":\\"EXECUTION_ENVIRONMENT\\"}": {
                                                              ".": {},
                                                              "f:name": {},
                                                              "f:value": {}
                                                          },
                                                          "k:{\\"name\\":\\"NAMESPACE\\"}": {
                                                              ".": {},
                                                              "f:name": {},
                                                              "f:valueFrom": {
                                                                  ".": {},
                                                                  "f:fieldRef": {}
                                                              }
                                                          },
                                                          "k:{\\"name\\":\\"TARGET_TIMEZONE\\"}": {
                                                              ".": {},
                                                              "f:name": {},
                                                              "f:value": {}
                                                          }
                                                      },
                                                      "f:image": {},
                                                      "f:imagePullPolicy": {},
                                                      "f:name": {},
                                                      "f:ports": {
                                                          ".": {},
                                                          "k:{\\"containerPort\\":9090,\\"protocol\\":\\"TCP\\"}": {
                                                              ".": {},
                                                              "f:containerPort": {},
                                                              "f:name": {},
                                                              "f:protocol": {}
                                                          }
                                                      },
                                                      "f:resources": {
                                                          ".": {},
                                                          "f:limits": {
                                                              ".": {},
                                                              "f:memory": "512Mi"
                                                          },
                                                          "f:requests": {
                                                              ".": {},
                                                              "f:cpu": "100m",
                                                              "f:memory": "512Mi"
                                                          }
                                                      },
                                                      "f:terminationMessagePath": {},
                                                      "f:terminationMessagePolicy": {},
                                                      "f:volumeMounts": {
                                                          ".": {},
                                                          "k:{\\"mountPath\\":\\"/app_config\\"}": {
                                                              ".": {},
                                                              "f:mountPath": {},
                                                              "f:name": {},
                                                              "f:readOnly": {}
                                                          },
                                                          "k:{\\"mountPath\\":\\"/app_config/base\\"}": {
                                                              ".": {},
                                                              "f:mountPath": {},
                                                              "f:name": {},
                                                              "f:readOnly": {}
                                                          },
                                                          "k:{\\"mountPath\\":\\"/app_secrets\\"}": {
                                                              ".": {},
                                                              "f:mountPath": {},
                                                              "f:name": {},
                                                              "f:readOnly": {}
                                                          }
                                                      }
                                                  }
                                              },
                                              "f:dnsPolicy": {},
                                              "f:restartPolicy": {},
                                              "f:schedulerName": {},
                                              "f:securityContext": {},
                                              "f:terminationGracePeriodSeconds": {},
                                              "f:volumes": {
                                                  ".": {},
                                                  "k:{\\"name\\":\\"app-secrets\\"}": {
                                                      ".": {},
                                                      "f:name": {},
                                                      "f:secret": {
                                                          ".": {},
                                                          "f:defaultMode": {},
                                                          "f:secretName": {}
                                                      }
                                                  },
                                                  "k:{\\"name\\":\\"config-map\\"}": {
                                                      ".": {},
                                                      "f:configMap": {
                                                          ".": {},
                                                          "f:defaultMode": {},
                                                          "f:name": {}
                                                      },
                                                      "f:name": {}
                                                  },
                                                  "k:{\\"name\\":\\"config-map-base\\"}": {
                                                      ".": {},
                                                      "f:configMap": {
                                                          ".": {},
                                                          "f:defaultMode": {},
                                                          "f:name": {}
                                                      },
                                                      "f:name": {}
                                                  }
                                              }
                                          }
                                      },
                                      "f:ttlSecondsAfterFinished": {}
                                  }
                              },
                              "manager": "kube-controller-manager",
                              "operation": "Update",
                              "subresource": null,
                              "time": 1744315800
                          },
                          {
                              "apiVersion": "batch/v1",
                              "fieldsType": "FieldsV1",
                              "fieldsV1": {
                                  "f:status": {
                                      "f:completionTime": {},
                                      "f:conditions": {},
                                      "f:ready": {},
                                      "f:startTime": {},
                                      "f:succeeded": {},
                                      "f:terminating": {},
                                      "f:uncountedTerminatedPods": {}
                                  }
                              },
                              "manager": "kube-controller-manager",
                              "operation": "Update",
                              "subresource": "status",
                              "time": 1744315948
                          }
                      ],
                      "name": "vis-intel-scraper-et-29071930",
                      "namespace": "visibility-intelligence",
                      "ownerReferences": [
                          {
                              "apiVersion": "batch/v1",
                              "blockOwnerDeletion": true,
                              "controller": true,
                              "kind": "CronJob",
                              "name": "vis-intel-scraper-et",
                              "uid": "1d0a33cc-b564-47f6-abbb-743a7bcd5aae"
                          }
                      ],
                      "resourceVersion": "15677416411",
                      "selfLink": null,
                      "uid": "06d43898-8e6f-4d14-9425-43b2a0bf59a2"
                  },
                  "spec": {
                      "activeDeadlineSeconds": 86400,
                      "backoffLimit": 6,
                      "completionMode": "NonIndexed",
                      "completions": 1,
                      "manualSelector": false,
                      "parallelism": 1,
                      "podFailurePolicy": null,
                      "selector": {
                          "matchExpressions": null,
                          "matchLabels": {
                              "batch.kubernetes.io/controller-uid": "06d43898-8e6f-4d14-9425-43b2a0bf59a2"
                          }
                      },
                      "suspend": false,
                      "template": {
                          "metadata": {
                              "annotations": {
                                  "appVersion": "c3d22ea8b598ae553eddd6e89c53115be8a84ca6",
                                  "cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
                                  "prometheus.io/port": "9090",
                                  "prometheus.io/scrape": "true"
                              },
                              "creationTimestamp": null,
                              "deletionGracePeriodSeconds": null,
                              "deletionTimestamp": null,
                              "finalizers": null,
                              "generateName": null,
                              "generation": null,
                              "labels": {
                                  "app": "vis-intel-scraper-et",
                                  "batch.kubernetes.io/controller-uid": "06d43898-8e6f-4d14-9425-43b2a0bf59a2",
                                  "batch.kubernetes.io/job-name": "vis-intel-scraper-et-29071930",
                                  "chart": "cron-application-1.0.0",
                                  "controller-uid": "06d43898-8e6f-4d14-9425-43b2a0bf59a2",
                                  "heritage": "Tiller",
                                  "istio-locality": "centralus",
                                  "job-name": "vis-intel-scraper-et-29071930",
                                  "release": "vis-intel-scraper-et-dvi3o0ai38lwgey3gwobsr0qv"
                              },
                              "managedFields": null,
                              "name": null,
                              "namespace": null,
                              "ownerReferences": null,
                              "resourceVersion": null,
                              "selfLink": null,
                              "uid": null
                          },
                          "spec": {
                              "activeDeadlineSeconds": null,
                              "affinity": null,
                              "automountServiceAccountToken": null,
                              "containers": [
                                  {
                                      "args": null,
                                      "command": null,
                                      "env": [
                                          {
                                              "name": "NAMESPACE",
                                              "value": null,
                                              "valueFrom": {
                                                  "configMapKeyRef": null,
                                                  "fieldRef": {
                                                      "apiVersion": "v1",
                                                      "fieldPath": "metadata.namespace"
                                                  },
                                                  "resourceFieldRef": null,
                                                  "secretKeyRef": null
                                              }
                                          },
                                          {
                                              "name": "APP_NAME",
                                              "value": "vis-intel-scraper-et",
                                              "valueFrom": null
                                          },
                                          {
                                              "name": "APP_REGION",
                                              "value": "centralus",
                                              "valueFrom": null
                                          },
                                          {
                                              "name": "EXECUTION_ENVIRONMENT",
                                              "value": "production",
                                              "valueFrom": null
                                          },
                                          {
                                              "name": "TARGET_TIMEZONE",
                                              "value": "America/New_York",
                                              "valueFrom": null
                                          }
                                      ],
                                      "envFrom": null,
                                      "image": "cssacrprod.azurecr.io/vis_intel_scraper:c3d22ea8b598ae553eddd6e89c53115be8a84ca6",
                                      "imagePullPolicy": "IfNotPresent",
                                      "lifecycle": null,
                                      "livenessProbe": null,
                                      "name": "vis-intel-scraper-et",
                                      "ports": [
                                          {
                                              "containerPort": 9090,
                                              "hostIP": null,
                                              "hostPort": null,
                                              "name": "prometheus",
                                              "protocol": "TCP"
                                          }
                                      ],
                                      "readinessProbe": null,
                                      "resources": {
                                          "limits": {
                                              "memory": "512Mi"
                                          },
                                          "requests": {
                                              "cpu": "100m",
                                              "memory": "512Mi"
                                          }
                                      },
                                      "securityContext": null,
                                      "startupProbe": null,
                                      "stdin": null,
                                      "stdinOnce": null,
                                      "terminationMessagePath": "/dev/termination-log",
                                      "terminationMessagePolicy": "File",
                                      "tty": null,
                                      "volumeDevices": null,
                                      "volumeMounts": [
                                          {
                                              "mountPath": "/app_secrets",
                                              "mountPropagation": null,
                                              "name": "app-secrets",
                                              "readOnly": true,
                                              "subPath": null,
                                              "subPathExpr": null
                                          },
                                          {
                                              "mountPath": "/app_config",
                                              "mountPropagation": null,
                                              "name": "config-map",
                                              "readOnly": true,
                                              "subPath": null,
                                              "subPathExpr": null
                                          },
                                          {
                                              "mountPath": "/app_config/base",
                                              "mountPropagation": null,
                                              "name": "config-map-base",
                                              "readOnly": true,
                                              "subPath": null,
                                              "subPathExpr": null
                                          }
                                      ],
                                      "workingDir": null
                                  }
                              ],
                              "dnsConfig": null,
                              "dnsPolicy": "ClusterFirst",
                              "enableServiceLinks": null,
                              "ephemeralContainers": null,
                              "hostAliases": null,
                              "hostIPC": null,
                              "hostNetwork": null,
                              "hostPID": null,
                              "hostUsers": null,
                              "hostname": null,
                              "imagePullSecrets": null,
                              "initContainers": null,
                              "nodeName": null,
                              "nodeSelector": null,
                              "os": null,
                              "overhead": null,
                              "preemptionPolicy": null,
                              "priority": null,
                              "priorityClassName": null,
                              "readinessGates": null,
                              "restartPolicy": "Never",
                              "runtimeClassName": null,
                              "schedulerName": "default-scheduler",
                              "securityContext": {
                                  "fsGroup": null,
                                  "fsGroupChangePolicy": null,
                                  "runAsGroup": null,
                                  "runAsNonRoot": null,
                                  "runAsUser": null,
                                  "seLinuxOptions": null,
                                  "seccompProfile": null,
                                  "supplementalGroups": null,
                                  "sysctls": null,
                                  "windowsOptions": null
                              },
                              "serviceAccount": null,
                              "serviceAccountName": null,
                              "setHostnameAsFQDN": null,
                              "shareProcessNamespace": null,
                              "subdomain": null,
                              "terminationGracePeriodSeconds": 30,
                              "tolerations": null,
                              "topologySpreadConstraints": null,
                              "volumes": [
                                  {
                                      "awsElasticBlockStore": null,
                                      "azureDisk": null,
                                      "azureFile": null,
                                      "cephfs": null,
                                      "cinder": null,
                                      "configMap": null,
                                      "csi": null,
                                      "downwardAPI": null,
                                      "emptyDir": null,
                                      "ephemeral": null,
                                      "fc": null,
                                      "flexVolume": null,
                                      "flocker": null,
                                      "gcePersistentDisk": null,
                                      "gitRepo": null,
                                      "glusterfs": null,
                                      "hostPath": null,
                                      "iscsi": null,
                                      "name": "app-secrets",
                                      "nfs": null,
                                      "persistentVolumeClaim": null,
                                      "photonPersistentDisk": null,
                                      "portworxVolume": null,
                                      "projected": null,
                                      "quobyte": null,
                                      "rbd": null,
                                      "scaleIO": null,
                                      "secret": {
                                          "defaultMode": 420,
                                          "items": null,
                                          "optional": null,
                                          "secretName": "vis-intel-jobs-config"
                                      },
                                      "storageos": null,
                                      "vsphereVolume": null
                                  },
                                  {
                                      "awsElasticBlockStore": null,
                                      "azureDisk": null,
                                      "azureFile": null,
                                      "cephfs": null,
                                      "cinder": null,
                                      "configMap": {
                                          "defaultMode": 420,
                                          "items": null,
                                          "name": "config-map",
                                          "optional": null
                                      },
                                      "csi": null,
                                      "downwardAPI": null,
                                      "emptyDir": null,
                                      "ephemeral": null,
                                      "fc": null,
                                      "flexVolume": null,
                                      "flocker": null,
                                      "gcePersistentDisk": null,
                                      "gitRepo": null,
                                      "glusterfs": null,
                                      "hostPath": null,
                                      "iscsi": null,
                                      "name": "config-map",
                                      "nfs": null,
                                      "persistentVolumeClaim": null,
                                      "photonPersistentDisk": null,
                                      "portworxVolume": null,
                                      "projected": null,
                                      "quobyte": null,
                                      "rbd": null,
                                      "scaleIO": null,
                                      "secret": null,
                                      "storageos": null,
                                      "vsphereVolume": null
                                  },
                                  {
                                      "awsElasticBlockStore": null,
                                      "azureDisk": null,
                                      "azureFile": null,
                                      "cephfs": null,
                                      "cinder": null,
                                      "configMap": {
                                          "defaultMode": 420,
                                          "items": null,
                                          "name": "config-map-base",
                                          "optional": null
                                      },
                                      "csi": null,
                                      "downwardAPI": null,
                                      "emptyDir": null,
                                      "ephemeral": null,
                                      "fc": null,
                                      "flexVolume": null,
                                      "flocker": null,
                                      "gcePersistentDisk": null,
                                      "gitRepo": null,
                                      "glusterfs": null,
                                      "hostPath": null,
                                      "iscsi": null,
                                      "name": "config-map-base",
                                      "nfs": null,
                                      "persistentVolumeClaim": null,
                                      "photonPersistentDisk": null,
                                      "portworxVolume": null,
                                      "projected": null,
                                      "quobyte": null,
                                      "rbd": null,
                                      "scaleIO": null,
                                      "secret": null,
                                      "storageos": null,
                                      "vsphereVolume": null
                                  }
                              ]
                          }
                      },
                      "ttlSecondsAfterFinished": 345600
                  },
                  "status": {
                      "active": null,
                      "completedIndexes": null,
                      "completionTime": 1744315948,
                      "conditions": [
                          {
                              "lastProbeTime": 1744315948,
                              "lastTransitionTime": 1744315948,
                              "message": null,
                              "reason": null,
                              "status": "True",
                              "type": "Complete"
                          }
                      ],
                      "failed": null,
                      "ready": 0,
                      "startTime": 1744315801,
                      "succeeded": 1,
                      "uncountedTerminatedPods": {
                          "failed": null,
                          "succeeded": null
                      }
                  }
              }
            """;

    @BeforeClass
    public void setup()
    {
        k8sClient = new K8SClient(new K8SConfig(), TESTING_TYPE_MANAGER);
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        // Set timezone to UTC for consistent timestamp handling using ZoneId
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("UTC")));
    }

    @Test
    public void testConvertJobWithTimestamps() throws Exception
    {
        // Parse test JSON into V1Job object
        V1Job job = objectMapper.readValue(TEST_JOB_JSON, V1Job.class);

        // Convert to Trino map
        Object converted = k8sClient.convertToMap(job);
        assertThat(converted).isInstanceOf(Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) converted;

        // Verify metadata timestamps
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) result.get("metadata");
        assertThat(metadata).isNotNull();

        // Verify creationTimestamp
        Object creationTimestamp = metadata.get("creationTimestamp");
        assertThat(creationTimestamp)
                .isInstanceOf(Timestamp.class)
                .extracting(ts -> ((Timestamp) ts).toInstant())
                .isEqualTo(Instant.ofEpochSecond(1744315801));

        // Verify managedFields timestamp
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> managedFields = (List<Map<String, Object>>) metadata.get("managedFields");
        assertThat(managedFields)
                .isNotNull()
                .hasSize(2);

        // Verify first managed field (update with no subresource)
        Object firstManagedFieldTime = managedFields.get(0).get("time");
        assertThat(firstManagedFieldTime)
                .isInstanceOf(Timestamp.class)
                .extracting(ts -> ((Timestamp) ts).toInstant())
                .isEqualTo(Instant.ofEpochSecond(1744315800));
        assertThat(managedFields.get(0).get("subresource")).isNull();

        // Verify second managed field (status update)
        Object secondManagedFieldTime = managedFields.get(1).get("time");
        assertThat(secondManagedFieldTime)
                .isInstanceOf(Timestamp.class)
                .extracting(ts -> ((Timestamp) ts).toInstant())
                .isEqualTo(Instant.ofEpochSecond(1744315948));
        assertThat(managedFields.get(1).get("subresource")).isEqualTo("status");

        // Verify status timestamps
        @SuppressWarnings("unchecked")
        Map<String, Object> status = (Map<String, Object>) result.get("status");
        assertThat(status).isNotNull();

        // Verify completionTime
        Object completionTime = status.get("completionTime");
        assertThat(completionTime)
                .isInstanceOf(Timestamp.class)
                .extracting(ts -> ((Timestamp) ts).toInstant())
                .isEqualTo(Instant.ofEpochSecond(1744315948));

        // Verify startTime
        Object startTime = status.get("startTime");
        assertThat(startTime)
                .isInstanceOf(Timestamp.class)
                .extracting(ts -> ((Timestamp) ts).toInstant())
                .isEqualTo(Instant.ofEpochSecond(1744315801));

        // Verify conditions timestamps
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> conditions = (List<Map<String, Object>>) status.get("conditions");
        assertThat(conditions)
                .isNotNull()
                .hasSize(1);

        Map<String, Object> condition = conditions.get(0);

        Object lastProbeTime = condition.get("lastProbeTime");
        assertThat(lastProbeTime)
                .isInstanceOf(Timestamp.class)
                .extracting(ts -> ((Timestamp) ts).toInstant())
                .isEqualTo(Instant.ofEpochSecond(1744315948));

        Object lastTransitionTime = condition.get("lastTransitionTime");
        assertThat(lastTransitionTime)
                .isInstanceOf(Timestamp.class)
                .extracting(ts -> ((Timestamp) ts).toInstant())
                .isEqualTo(Instant.ofEpochSecond(1744315948));
    }

    @Test
    public void testConvertTimestamp()
    {
        Map<String, Object> input = new HashMap<>();
        long epochSeconds = 1234567890L;
        input.put("creationTimestamp", epochSeconds);
        
        Object result = k8sClient.convertToMap(input);
        assertThat(result).isInstanceOf(Map.class);
        
        Map<String, Object> resultMap = (Map<String, Object>) result;
        Object timestamp = resultMap.get("creationTimestamp");
        assertThat(timestamp)
            .isInstanceOf(java.sql.Timestamp.class)
            .isEqualTo(new java.sql.Timestamp(epochSeconds * 1000));
    }

    private Map<String, Object> createTestPod()
    {
        return new HashMap<>();
    }
}
