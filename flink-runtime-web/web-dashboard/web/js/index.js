angular.module('flinkApp', ['ui.router', 'angularMoment', 'dndLists']).run(["$rootScope", function($rootScope) {
  $rootScope.sidebarVisible = false;
  return $rootScope.showSidebar = function() {
    $rootScope.sidebarVisible = !$rootScope.sidebarVisible;
    return $rootScope.sidebarClass = 'force-show';
  };
}]).value('flinkConfig', {
  jobServer: '',
  "refresh-interval": 10000
}).run(["JobsService", "MainService", "flinkConfig", "$interval", function(JobsService, MainService, flinkConfig, $interval) {
  return MainService.loadConfig().then(function(config) {
    angular.extend(flinkConfig, config);
    JobsService.listJobs();
    return $interval(function() {
      return JobsService.listJobs();
    }, flinkConfig["refresh-interval"]);
  });
}]).config(["$uiViewScrollProvider", function($uiViewScrollProvider) {
  return $uiViewScrollProvider.useAnchorScroll();
}]).run(["$rootScope", "$state", function($rootScope, $state) {
  return $rootScope.$on('$stateChangeStart', function(event, toState, toParams, fromState) {
    if (toState.redirectTo) {
      event.preventDefault();
      return $state.go(toState.redirectTo, toParams);
    }
  });
}]).config(["$stateProvider", "$urlRouterProvider", function($stateProvider, $urlRouterProvider) {
  $stateProvider.state("overview", {
    url: "/overview",
    views: {
      main: {
        templateUrl: "partials/overview.html",
        controller: 'OverviewController'
      }
    }
  }).state("running-jobs", {
    url: "/running-jobs",
    views: {
      main: {
        templateUrl: "partials/jobs/running-jobs.html",
        controller: 'RunningJobsController'
      }
    }
  }).state("completed-jobs", {
    url: "/completed-jobs",
    views: {
      main: {
        templateUrl: "partials/jobs/completed-jobs.html",
        controller: 'CompletedJobsController'
      }
    }
  }).state("single-job", {
    url: "/jobs/{jobid}",
    abstract: true,
    views: {
      main: {
        templateUrl: "partials/jobs/job.html",
        controller: 'SingleJobController'
      }
    }
  }).state("single-job.plan", {
    url: "",
    redirectTo: "single-job.plan.subtasks",
    views: {
      details: {
        templateUrl: "partials/jobs/job.plan.html",
        controller: 'JobPlanController'
      }
    }
  }).state("single-job.plan.subtasks", {
    url: "",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.subtasks.html",
        controller: 'JobPlanSubtasksController'
      }
    }
  }).state("single-job.plan.metrics", {
    url: "/metrics",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.metrics.html",
        controller: 'JobPlanMetricsController'
      }
    }
  }).state("single-job.plan.taskmanagers", {
    url: "/taskmanagers",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.taskmanagers.html",
        controller: 'JobPlanTaskManagersController'
      }
    }
  }).state("single-job.plan.accumulators", {
    url: "/accumulators",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.accumulators.html",
        controller: 'JobPlanAccumulatorsController'
      }
    }
  }).state("single-job.plan.checkpoints", {
    url: "/checkpoints",
    redirectTo: "single-job.plan.checkpoints.overview",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.checkpoints.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.checkpoints.overview", {
    url: "/overview",
    views: {
      'checkpoints-view': {
        templateUrl: "partials/jobs/job.plan.node.checkpoints.overview.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.checkpoints.summary", {
    url: "/summary",
    views: {
      'checkpoints-view': {
        templateUrl: "partials/jobs/job.plan.node.checkpoints.summary.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.checkpoints.history", {
    url: "/history",
    views: {
      'checkpoints-view': {
        templateUrl: "partials/jobs/job.plan.node.checkpoints.history.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.checkpoints.config", {
    url: "/config",
    views: {
      'checkpoints-view': {
        templateUrl: "partials/jobs/job.plan.node.checkpoints.config.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.checkpoints.details", {
    url: "/details/{checkpointId}",
    views: {
      'checkpoints-view': {
        templateUrl: "partials/jobs/job.plan.node.checkpoints.details.html",
        controller: 'JobPlanCheckpointDetailsController'
      }
    }
  }).state("single-job.plan.backpressure", {
    url: "/backpressure",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.backpressure.html",
        controller: 'JobPlanBackPressureController'
      }
    }
  }).state("single-job.timeline", {
    url: "/timeline",
    views: {
      details: {
        templateUrl: "partials/jobs/job.timeline.html"
      }
    }
  }).state("single-job.timeline.vertex", {
    url: "/{vertexId}",
    views: {
      vertex: {
        templateUrl: "partials/jobs/job.timeline.vertex.html",
        controller: 'JobTimelineVertexController'
      }
    }
  }).state("single-job.exceptions", {
    url: "/exceptions",
    views: {
      details: {
        templateUrl: "partials/jobs/job.exceptions.html",
        controller: 'JobExceptionsController'
      }
    }
  }).state("single-job.config", {
    url: "/config",
    views: {
      details: {
        templateUrl: "partials/jobs/job.config.html"
      }
    }
  }).state("all-manager", {
    url: "/taskmanagers",
    views: {
      main: {
        templateUrl: "partials/taskmanager/index.html",
        controller: 'AllTaskManagersController'
      }
    }
  }).state("single-manager", {
    url: "/taskmanager/{taskmanagerid}",
    abstract: true,
    views: {
      main: {
        templateUrl: "partials/taskmanager/taskmanager.html",
        controller: 'SingleTaskManagerController'
      }
    }
  }).state("single-manager.metrics", {
    url: "/metrics",
    views: {
      details: {
        templateUrl: "partials/taskmanager/taskmanager.metrics.html"
      }
    }
  }).state("single-manager.stdout", {
    url: "/stdout",
    views: {
      details: {
        templateUrl: "partials/taskmanager/taskmanager.stdout.html",
        controller: 'SingleTaskManagerStdoutController'
      }
    }
  }).state("single-manager.log", {
    url: "/log",
    views: {
      details: {
        templateUrl: "partials/taskmanager/taskmanager.log.html",
        controller: 'SingleTaskManagerLogsController'
      }
    }
  }).state("jobmanager", {
    url: "/jobmanager",
    views: {
      main: {
        templateUrl: "partials/jobmanager/index.html"
      }
    }
  }).state("jobmanager.config", {
    url: "/config",
    views: {
      details: {
        templateUrl: "partials/jobmanager/config.html",
        controller: 'JobManagerConfigController'
      }
    }
  }).state("jobmanager.stdout", {
    url: "/stdout",
    views: {
      details: {
        templateUrl: "partials/jobmanager/stdout.html",
        controller: 'JobManagerStdoutController'
      }
    }
  }).state("jobmanager.log", {
    url: "/log",
    views: {
      details: {
        templateUrl: "partials/jobmanager/log.html",
        controller: 'JobManagerLogsController'
      }
    }
  }).state("submit", {
    url: "/submit",
    views: {
      main: {
        templateUrl: "partials/submit.html",
        controller: "JobSubmitController"
      }
    }
  });
  return $urlRouterProvider.otherwise("/overview");
}]);

angular.module('flinkApp').directive('bsLabel', ["JobsService", function(JobsService) {
  return {
    transclude: true,
    replace: true,
    scope: {
      getLabelClass: "&",
      status: "@"
    },
    template: "<span title='{{status}}' ng-class='getLabelClass()'><ng-transclude></ng-transclude></span>",
    link: function(scope, element, attrs) {
      return scope.getLabelClass = function() {
        return 'label label-' + JobsService.translateLabelState(attrs.status);
      };
    }
  };
}]).directive('bpLabel', ["JobsService", function(JobsService) {
  return {
    transclude: true,
    replace: true,
    scope: {
      getBackPressureLabelClass: "&",
      status: "@"
    },
    template: "<span title='{{status}}' ng-class='getBackPressureLabelClass()'><ng-transclude></ng-transclude></span>",
    link: function(scope, element, attrs) {
      return scope.getBackPressureLabelClass = function() {
        return 'label label-' + JobsService.translateBackPressureLabelState(attrs.status);
      };
    }
  };
}]).directive('indicatorPrimary', ["JobsService", function(JobsService) {
  return {
    replace: true,
    scope: {
      getLabelClass: "&",
      status: '@'
    },
    template: "<i title='{{status}}' ng-class='getLabelClass()' />",
    link: function(scope, element, attrs) {
      return scope.getLabelClass = function() {
        return 'fa fa-circle indicator indicator-' + JobsService.translateLabelState(attrs.status);
      };
    }
  };
}]).directive('tableProperty', function() {
  return {
    replace: true,
    scope: {
      value: '='
    },
    template: "<td title=\"{{value || 'None'}}\">{{value || 'None'}}</td>"
  };
});

angular.module('flinkApp').filter("amDurationFormatExtended", ["angularMomentConfig", function(angularMomentConfig) {
  var amDurationFormatExtendedFilter;
  amDurationFormatExtendedFilter = function(value, format, durationFormat) {
    if (typeof value === "undefined" || value === null) {
      return "";
    }
    return moment.duration(value, format).format(durationFormat, {
      trim: false
    });
  };
  amDurationFormatExtendedFilter.$stateful = angularMomentConfig.statefulFilters;
  return amDurationFormatExtendedFilter;
}]).filter("humanizeDuration", function() {
  return function(value, short) {
    var days, hours, minutes, ms, seconds, x;
    if (typeof value === "undefined" || value === null) {
      return "";
    }
    ms = value % 1000;
    x = Math.floor(value / 1000);
    seconds = x % 60;
    x = Math.floor(x / 60);
    minutes = x % 60;
    x = Math.floor(x / 60);
    hours = x % 24;
    x = Math.floor(x / 24);
    days = x;
    if (days === 0) {
      if (hours === 0) {
        if (minutes === 0) {
          if (seconds === 0) {
            return ms + "ms";
          } else {
            return seconds + "s ";
          }
        } else {
          return minutes + "m " + seconds + "s";
        }
      } else {
        if (short) {
          return hours + "h " + minutes + "m";
        } else {
          return hours + "h " + minutes + "m " + seconds + "s";
        }
      }
    } else {
      if (short) {
        return days + "d " + hours + "h";
      } else {
        return days + "d " + hours + "h " + minutes + "m " + seconds + "s";
      }
    }
  };
}).filter("humanizeText", function() {
  return function(text) {
    if (text) {
      return text.replace(/&gt;/g, ">").replace(/<br\/>/g, "");
    } else {
      return '';
    }
  };
}).filter("humanizeBytes", function() {
  return function(bytes) {
    var converter, units;
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    converter = function(value, power) {
      var base;
      base = Math.pow(1024, power);
      if (value < base) {
        return (value / base).toFixed(2) + " " + units[power];
      } else if (value < base * 1000) {
        return (value / base).toPrecision(3) + " " + units[power];
      } else {
        return converter(value, power + 1);
      }
    };
    if (typeof bytes === "undefined" || bytes === null) {
      return "";
    }
    if (bytes < 1000) {
      return bytes + " B";
    } else {
      return converter(bytes, 1);
    }
  };
}).filter("toLocaleString", function() {
  return function(text) {
    return text.toLocaleString();
  };
}).filter("toUpperCase", function() {
  return function(text) {
    return text.toUpperCase();
  };
}).filter("percentage", function() {
  return function(number) {
    return (number * 100).toFixed(0) + '%';
  };
});

angular.module('flinkApp').service('MainService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "config").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('JobManagerConfigController', ["$scope", "JobManagerConfigService", function($scope, JobManagerConfigService) {
  return JobManagerConfigService.loadConfig().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['config'] = data;
  });
}]).controller('JobManagerLogsController', ["$scope", "JobManagerLogsService", function($scope, JobManagerLogsService) {
  JobManagerLogsService.loadLogs().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['log'] = data;
  });
  return $scope.reloadData = function() {
    return JobManagerLogsService.loadLogs().then(function(data) {
      return $scope.jobmanager['log'] = data;
    });
  };
}]).controller('JobManagerStdoutController', ["$scope", "JobManagerStdoutService", function($scope, JobManagerStdoutService) {
  JobManagerStdoutService.loadStdout().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['stdout'] = data;
  });
  return $scope.reloadData = function() {
    return JobManagerStdoutService.loadStdout().then(function(data) {
      return $scope.jobmanager['stdout'] = data;
    });
  };
}]);

angular.module('flinkApp').service('JobManagerConfigService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var config;
  config = {};
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobmanager/config").success(function(data, status, headers, config) {
      config = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]).service('JobManagerLogsService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var logs;
  logs = {};
  this.loadLogs = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobmanager/log").success(function(data, status, headers, config) {
      logs = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]).service('JobManagerStdoutService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var stdout;
  stdout = {};
  this.loadStdout = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobmanager/stdout").success(function(data, status, headers, config) {
      stdout = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('RunningJobsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.jobObserver = function() {
    return $scope.jobs = JobsService.getJobs('running');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  return $scope.jobObserver();
}]).controller('CompletedJobsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.jobObserver = function() {
    return $scope.jobs = JobsService.getJobs('finished');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  return $scope.jobObserver();
}]).controller('SingleJobController', ["$scope", "$state", "$stateParams", "JobsService", "MetricsService", "$rootScope", "flinkConfig", "$interval", function($scope, $state, $stateParams, JobsService, MetricsService, $rootScope, flinkConfig, $interval) {
  var refresher;
  $scope.jobid = $stateParams.jobid;
  $scope.job = null;
  $scope.plan = null;
  $scope.vertices = null;
  $scope.backPressureOperatorStats = {};
  JobsService.loadJob($stateParams.jobid).then(function(data) {
    $scope.job = data;
    $scope.plan = data.plan;
    $scope.vertices = data.vertices;
    return MetricsService.setupMetrics($stateParams.jobid, data.vertices);
  });
  refresher = $interval(function() {
    return JobsService.loadJob($stateParams.jobid).then(function(data) {
      $scope.job = data;
      return $scope.$broadcast('reload');
    });
  }, flinkConfig["refresh-interval"]);
  $scope.$on('$destroy', function() {
    $scope.job = null;
    $scope.plan = null;
    $scope.vertices = null;
    $scope.backPressureOperatorStats = null;
    return $interval.cancel(refresher);
  });
  $scope.cancelJob = function(cancelEvent) {
    angular.element(cancelEvent.currentTarget).removeClass("btn").removeClass("btn-default").html('Cancelling...');
    return JobsService.cancelJob($stateParams.jobid).then(function(data) {
      return {};
    });
  };
  return $scope.stopJob = function(stopEvent) {
    angular.element(stopEvent.currentTarget).removeClass("btn").removeClass("btn-default").html('Stopping...');
    return JobsService.stopJob($stateParams.jobid).then(function(data) {
      return {};
    });
  };
}]).controller('JobPlanController', ["$scope", "$state", "$stateParams", "$window", "JobsService", function($scope, $state, $stateParams, $window, JobsService) {
  $scope.nodeid = null;
  $scope.nodeUnfolded = false;
  $scope.stateList = JobsService.stateList();
  $scope.changeNode = function(nodeid) {
    if (nodeid !== $scope.nodeid) {
      $scope.nodeid = nodeid;
      $scope.vertex = null;
      $scope.subtasks = null;
      $scope.accumulators = null;
      $scope.operatorCheckpointStats = null;
      $scope.$broadcast('reload');
      return $scope.$broadcast('node:change', $scope.nodeid);
    } else {
      $scope.nodeid = null;
      $scope.nodeUnfolded = false;
      $scope.vertex = null;
      $scope.subtasks = null;
      $scope.accumulators = null;
      return $scope.operatorCheckpointStats = null;
    }
  };
  $scope.deactivateNode = function() {
    $scope.nodeid = null;
    $scope.nodeUnfolded = false;
    $scope.vertex = null;
    $scope.subtasks = null;
    $scope.accumulators = null;
    return $scope.operatorCheckpointStats = null;
  };
  return $scope.toggleFold = function() {
    return $scope.nodeUnfolded = !$scope.nodeUnfolded;
  };
}]).controller('JobPlanSubtasksController', ["$scope", "JobsService", function($scope, JobsService) {
  var getSubtasks;
  getSubtasks = function() {
    return JobsService.getSubtasks($scope.nodeid).then(function(data) {
      return $scope.subtasks = data;
    });
  };
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.st)) {
    getSubtasks();
  }
  return $scope.$on('reload', function(event) {
    if ($scope.nodeid) {
      return getSubtasks();
    }
  });
}]).controller('JobPlanTaskManagersController', ["$scope", "JobsService", function($scope, JobsService) {
  var getTaskManagers;
  getTaskManagers = function() {
    return JobsService.getTaskManagers($scope.nodeid).then(function(data) {
      return $scope.taskmanagers = data;
    });
  };
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.st)) {
    getTaskManagers();
  }
  return $scope.$on('reload', function(event) {
    if ($scope.nodeid) {
      return getTaskManagers();
    }
  });
}]).controller('JobPlanAccumulatorsController', ["$scope", "JobsService", function($scope, JobsService) {
  var getAccumulators;
  getAccumulators = function() {
    return JobsService.getAccumulators($scope.nodeid).then(function(data) {
      $scope.accumulators = data.main;
      return $scope.subtaskAccumulators = data.subtasks;
    });
  };
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.accumulators)) {
    getAccumulators();
  }
  return $scope.$on('reload', function(event) {
    if ($scope.nodeid) {
      return getAccumulators();
    }
  });
}]).controller('JobPlanCheckpointsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  var getGeneralCheckpointStats;
  $scope.checkpointDetails = {};
  $scope.checkpointDetails.id = -1;
  JobsService.getCheckpointConfig().then(function(data) {
    return $scope.checkpointConfig = data;
  });
  getGeneralCheckpointStats = function() {
    return JobsService.getCheckpointStats().then(function(data) {
      if (data !== null) {
        return $scope.checkpointStats = data;
      }
    });
  };
  getGeneralCheckpointStats();
  return $scope.$on('reload', function(event) {
    return getGeneralCheckpointStats();
  });
}]).controller('JobPlanCheckpointDetailsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  var getCheckpointDetails, getCheckpointSubtaskDetails;
  $scope.subtaskDetails = {};
  $scope.checkpointDetails.id = $stateParams.checkpointId;
  getCheckpointDetails = function(checkpointId) {
    return JobsService.getCheckpointDetails(checkpointId).then(function(data) {
      if (data !== null) {
        return $scope.checkpoint = data;
      } else {
        return $scope.unknown_checkpoint = true;
      }
    });
  };
  getCheckpointSubtaskDetails = function(checkpointId, vertexId) {
    return JobsService.getCheckpointSubtaskDetails(checkpointId, vertexId).then(function(data) {
      if (data !== null) {
        return $scope.subtaskDetails[vertexId] = data;
      }
    });
  };
  getCheckpointDetails($stateParams.checkpointId);
  if ($scope.nodeid) {
    getCheckpointSubtaskDetails($stateParams.checkpointId, $scope.nodeid);
  }
  $scope.$on('reload', function(event) {
    getCheckpointDetails($stateParams.checkpointId);
    if ($scope.nodeid) {
      return getCheckpointSubtaskDetails($stateParams.checkpointId, $scope.nodeid);
    }
  });
  return $scope.$on('$destroy', function() {
    return $scope.checkpointDetails.id = -1;
  });
}]).controller('JobPlanBackPressureController', ["$scope", "JobsService", function($scope, JobsService) {
  var getOperatorBackPressure;
  getOperatorBackPressure = function() {
    $scope.now = Date.now();
    if ($scope.nodeid) {
      return JobsService.getOperatorBackPressure($scope.nodeid).then(function(data) {
        return $scope.backPressureOperatorStats[$scope.nodeid] = data;
      });
    }
  };
  getOperatorBackPressure();
  return $scope.$on('reload', function(event) {
    return getOperatorBackPressure();
  });
}]).controller('JobTimelineVertexController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  var getVertex;
  getVertex = function() {
    return JobsService.getVertex($stateParams.vertexId).then(function(data) {
      return $scope.vertex = data;
    });
  };
  getVertex();
  return $scope.$on('reload', function(event) {
    return getVertex();
  });
}]).controller('JobExceptionsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.loadExceptions().then(function(data) {
    return $scope.exceptions = data;
  });
}]).controller('JobPropertiesController', ["$scope", "JobsService", function($scope, JobsService) {
  return $scope.changeNode = function(nodeid) {
    if (nodeid !== $scope.nodeid) {
      $scope.nodeid = nodeid;
      return JobsService.getNode(nodeid).then(function(data) {
        return $scope.node = data;
      });
    } else {
      $scope.nodeid = null;
      return $scope.node = null;
    }
  };
}]).controller('JobPlanMetricsController', ["$scope", "JobsService", "MetricsService", function($scope, JobsService, MetricsService) {
  var loadMetrics;
  $scope.dragging = false;
  $scope.window = MetricsService.getWindow();
  $scope.availableMetrics = null;
  $scope.$on('$destroy', function() {
    return MetricsService.unRegisterObserver();
  });
  loadMetrics = function() {
    JobsService.getVertex($scope.nodeid).then(function(data) {
      return $scope.vertex = data;
    });
    return MetricsService.getAvailableMetrics($scope.jobid, $scope.nodeid).then(function(data) {
      $scope.availableMetrics = data;
      $scope.metrics = MetricsService.getMetricsSetup($scope.jobid, $scope.nodeid).names;
      return MetricsService.registerObserver($scope.jobid, $scope.nodeid, function(data) {
        return $scope.$broadcast("metrics:data:update", data.timestamp, data.values);
      });
    });
  };
  $scope.dropped = function(event, index, item, external, type) {
    MetricsService.orderMetrics($scope.jobid, $scope.nodeid, item, index);
    $scope.$broadcast("metrics:refresh", item);
    loadMetrics();
    return false;
  };
  $scope.dragStart = function() {
    return $scope.dragging = true;
  };
  $scope.dragEnd = function() {
    return $scope.dragging = false;
  };
  $scope.addMetric = function(metric) {
    MetricsService.addMetric($scope.jobid, $scope.nodeid, metric.id);
    return loadMetrics();
  };
  $scope.removeMetric = function(metric) {
    MetricsService.removeMetric($scope.jobid, $scope.nodeid, metric);
    return loadMetrics();
  };
  $scope.setMetricSize = function(metric, size) {
    MetricsService.setMetricSize($scope.jobid, $scope.nodeid, metric, size);
    return loadMetrics();
  };
  $scope.getValues = function(metric) {
    return MetricsService.getValues($scope.jobid, $scope.nodeid, metric);
  };
  $scope.$on('node:change', function(event, nodeid) {
    if (!$scope.dragging) {
      return loadMetrics();
    }
  });
  if ($scope.nodeid) {
    return loadMetrics();
  }
}]);

angular.module('flinkApp').directive('vertex', ["$state", function($state) {
  return {
    template: "<svg class='timeline secondary' width='0' height='0'></svg>",
    scope: {
      data: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl;
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW);
      analyzeTime = function(data) {
        var chart, svg, testData;
        d3.select(svgEl).selectAll("*").remove();
        testData = [];
        angular.forEach(data.subtasks, function(subtask, i) {
          var times;
          times = [
            {
              label: "Scheduled",
              color: "#666",
              borderColor: "#555",
              starting_time: subtask.timestamps["SCHEDULED"],
              ending_time: subtask.timestamps["DEPLOYING"],
              type: 'regular'
            }, {
              label: "Deploying",
              color: "#aaa",
              borderColor: "#555",
              starting_time: subtask.timestamps["DEPLOYING"],
              ending_time: subtask.timestamps["RUNNING"],
              type: 'regular'
            }
          ];
          if (subtask.timestamps["FINISHED"] > 0) {
            times.push({
              label: "Running",
              color: "#ddd",
              borderColor: "#555",
              starting_time: subtask.timestamps["RUNNING"],
              ending_time: subtask.timestamps["FINISHED"],
              type: 'regular'
            });
          }
          return testData.push({
            label: "(" + subtask.subtask + ") " + subtask.host,
            times: times
          });
        });
        chart = d3.timeline().stack().tickFormat({
          format: d3.time.format("%L"),
          tickSize: 1
        }).prefix("single").labelFormat(function(label) {
          return label;
        }).margin({
          left: 100,
          right: 0,
          top: 0,
          bottom: 0
        }).itemHeight(30).relativeTime();
        return svg = d3.select(svgEl).datum(testData).call(chart);
      };
      analyzeTime(scope.data);
    }
  };
}]).directive('timeline', ["$state", function($state) {
  return {
    template: "<svg class='timeline' width='0' height='0'></svg>",
    scope: {
      vertices: "=",
      jobid: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl, translateLabel;
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW);
      translateLabel = function(label) {
        return label.replace("&gt;", ">");
      };
      analyzeTime = function(data) {
        var chart, svg, testData;
        d3.select(svgEl).selectAll("*").remove();
        testData = [];
        angular.forEach(data, function(vertex) {
          if (vertex['start-time'] > -1) {
            if (vertex.type === 'scheduled') {
              return testData.push({
                times: [
                  {
                    label: translateLabel(vertex.name),
                    color: "#cccccc",
                    borderColor: "#555555",
                    starting_time: vertex['start-time'],
                    ending_time: vertex['end-time'],
                    type: vertex.type
                  }
                ]
              });
            } else {
              return testData.push({
                times: [
                  {
                    label: translateLabel(vertex.name),
                    color: "#d9f1f7",
                    borderColor: "#62cdea",
                    starting_time: vertex['start-time'],
                    ending_time: vertex['end-time'],
                    link: vertex.id,
                    type: vertex.type
                  }
                ]
              });
            }
          }
        });
        chart = d3.timeline().stack().click(function(d, i, datum) {
          if (d.link) {
            return $state.go("single-job.timeline.vertex", {
              jobid: scope.jobid,
              vertexId: d.link
            });
          }
        }).tickFormat({
          format: d3.time.format("%L"),
          tickSize: 1
        }).prefix("main").margin({
          left: 0,
          right: 0,
          top: 0,
          bottom: 0
        }).itemHeight(30).showBorderLine().showHourTimeline();
        return svg = d3.select(svgEl).datum(testData).call(chart);
      };
      scope.$watch(attrs.vertices, function(data) {
        if (data) {
          return analyzeTime(data);
        }
      });
    }
  };
}]).directive('split', function() {
  return {
    compile: function(tElem, tAttrs) {
      return Split(tElem.children(), {
        sizes: [50, 50],
        direction: 'vertical'
      });
    }
  };
}).directive('jobPlan', ["$timeout", function($timeout) {
  return {
    template: "<svg class='graph' width='500' height='400'><g /></svg> <svg class='tmp' width='1' height='1'><g /></svg> <div class='btn-group zoom-buttons'> <a class='btn btn-default zoom-in' ng-click='zoomIn()'><i class='fa fa-plus' /></a> <a class='btn btn-default zoom-out' ng-click='zoomOut()'><i class='fa fa-minus' /></a> </div>",
    scope: {
      plan: '=',
      setNode: '&'
    },
    link: function(scope, elem, attrs) {
      var containerW, createEdge, createLabelEdge, createLabelNode, createNode, d3mainSvg, d3mainSvgG, d3tmpSvg, drawGraph, extendLabelNodeForIteration, g, getNodeType, isSpecialIterationNode, jobid, loadJsonToDagre, mainG, mainSvgElement, mainTmpElement, mainZoom, searchForNode, shortenString, subgraphs;
      g = null;
      mainZoom = d3.behavior.zoom();
      subgraphs = [];
      jobid = attrs.jobid;
      mainSvgElement = elem.children()[0];
      mainG = elem.children().children()[0];
      mainTmpElement = elem.children()[1];
      d3mainSvg = d3.select(mainSvgElement);
      d3mainSvgG = d3.select(mainG);
      d3tmpSvg = d3.select(mainTmpElement);
      containerW = elem.width();
      angular.element(elem.children()[0]).width(containerW);
      scope.zoomIn = function() {
        var translate, v1, v2;
        if (mainZoom.scale() < 2.99) {
          translate = mainZoom.translate();
          v1 = translate[0] * (mainZoom.scale() + 0.1 / (mainZoom.scale()));
          v2 = translate[1] * (mainZoom.scale() + 0.1 / (mainZoom.scale()));
          mainZoom.scale(mainZoom.scale() + 0.1);
          mainZoom.translate([v1, v2]);
          return d3mainSvgG.attr("transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")");
        }
      };
      scope.zoomOut = function() {
        var translate, v1, v2;
        if (mainZoom.scale() > 0.31) {
          mainZoom.scale(mainZoom.scale() - 0.1);
          translate = mainZoom.translate();
          v1 = translate[0] * (mainZoom.scale() - 0.1 / (mainZoom.scale()));
          v2 = translate[1] * (mainZoom.scale() - 0.1 / (mainZoom.scale()));
          mainZoom.translate([v1, v2]);
          return d3mainSvgG.attr("transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")");
        }
      };
      createLabelEdge = function(el) {
        var labelValue;
        labelValue = "";
        if ((el.ship_strategy != null) || (el.local_strategy != null)) {
          labelValue += "<div class='edge-label'>";
          if (el.ship_strategy != null) {
            labelValue += el.ship_strategy;
          }
          if (el.temp_mode !== undefined) {
            labelValue += " (" + el.temp_mode + ")";
          }
          if (el.local_strategy !== undefined) {
            labelValue += ",<br>" + el.local_strategy;
          }
          labelValue += "</div>";
        }
        return labelValue;
      };
      isSpecialIterationNode = function(info) {
        return info === "partialSolution" || info === "nextPartialSolution" || info === "workset" || info === "nextWorkset" || info === "solutionSet" || info === "solutionDelta";
      };
      getNodeType = function(el, info) {
        if (info === "mirror") {
          return 'node-mirror';
        } else if (isSpecialIterationNode(info)) {
          return 'node-iteration';
        } else {
          return 'node-normal';
        }
      };
      createLabelNode = function(el, info, maxW, maxH) {
        var labelValue, stepName;
        labelValue = "<div href='#/jobs/" + jobid + "/vertex/" + el.id + "' class='node-label " + getNodeType(el, info) + "'>";
        if (info === "mirror") {
          labelValue += "<h3 class='node-name'>Mirror of " + el.operator + "</h3>";
        } else {
          labelValue += "<h3 class='node-name'>" + el.operator + "</h3>";
        }
        if (el.description === "") {
          labelValue += "";
        } else {
          stepName = el.description;
          stepName = shortenString(stepName);
          labelValue += "<h4 class='step-name'>" + stepName + "</h4>";
        }
        if (el.step_function != null) {
          labelValue += extendLabelNodeForIteration(el.id, maxW, maxH);
        } else {
          if (isSpecialIterationNode(info)) {
            labelValue += "<h5>" + info + " Node</h5>";
          }
          if (el.parallelism !== "") {
            labelValue += "<h5>Parallelism: " + el.parallelism + "</h5>";
          }
          if (!(el.operator === undefined || !el.operator_strategy)) {
            labelValue += "<h5>Operation: " + shortenString(el.operator_strategy) + "</h5>";
          }
        }
        labelValue += "</div>";
        return labelValue;
      };
      extendLabelNodeForIteration = function(id, maxW, maxH) {
        var labelValue, svgID;
        svgID = "svg-" + id;
        labelValue = "<svg class='" + svgID + "' width=" + maxW + " height=" + maxH + "><g /></svg>";
        return labelValue;
      };
      shortenString = function(s) {
        var sbr;
        if (s.charAt(0) === "<") {
          s = s.replace("<", "&lt;");
          s = s.replace(">", "&gt;");
        }
        sbr = "";
        while (s.length > 30) {
          sbr = sbr + s.substring(0, 30) + "<br>";
          s = s.substring(30, s.length);
        }
        sbr = sbr + s;
        return sbr;
      };
      createNode = function(g, data, el, isParent, maxW, maxH) {
        if (isParent == null) {
          isParent = false;
        }
        if (el.id === data.partial_solution) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "partialSolution", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "partialSolution")
          });
        } else if (el.id === data.next_partial_solution) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "nextPartialSolution", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "nextPartialSolution")
          });
        } else if (el.id === data.workset) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "workset", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "workset")
          });
        } else if (el.id === data.next_workset) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "nextWorkset", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "nextWorkset")
          });
        } else if (el.id === data.solution_set) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "solutionSet", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "solutionSet")
          });
        } else if (el.id === data.solution_delta) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "solutionDelta", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "solutionDelta")
          });
        } else {
          return g.setNode(el.id, {
            label: createLabelNode(el, "", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "")
          });
        }
      };
      createEdge = function(g, data, el, existingNodes, pred) {
        return g.setEdge(pred.id, el.id, {
          label: createLabelEdge(pred),
          labelType: 'html',
          arrowhead: 'normal'
        });
      };
      loadJsonToDagre = function(g, data) {
        var el, existingNodes, isParent, k, l, len, len1, maxH, maxW, pred, r, ref, sg, toIterate;
        existingNodes = [];
        if (data.nodes != null) {
          toIterate = data.nodes;
        } else {
          toIterate = data.step_function;
          isParent = true;
        }
        for (k = 0, len = toIterate.length; k < len; k++) {
          el = toIterate[k];
          maxW = 0;
          maxH = 0;
          if (el.step_function) {
            sg = new dagreD3.graphlib.Graph({
              multigraph: true,
              compound: true
            }).setGraph({
              nodesep: 20,
              edgesep: 0,
              ranksep: 20,
              rankdir: "LR",
              marginx: 10,
              marginy: 10
            });
            subgraphs[el.id] = sg;
            loadJsonToDagre(sg, el);
            r = new dagreD3.render();
            d3tmpSvg.select('g').call(r, sg);
            maxW = sg.graph().width;
            maxH = sg.graph().height;
            angular.element(mainTmpElement).empty();
          }
          createNode(g, data, el, isParent, maxW, maxH);
          existingNodes.push(el.id);
          if (el.inputs != null) {
            ref = el.inputs;
            for (l = 0, len1 = ref.length; l < len1; l++) {
              pred = ref[l];
              createEdge(g, data, el, existingNodes, pred);
            }
          }
        }
        return g;
      };
      searchForNode = function(data, nodeID) {
        var el, i, j;
        for (i in data.nodes) {
          el = data.nodes[i];
          if (el.id === nodeID) {
            return el;
          }
          if (el.step_function != null) {
            for (j in el.step_function) {
              if (el.step_function[j].id === nodeID) {
                return el.step_function[j];
              }
            }
          }
        }
      };
      drawGraph = function(data) {
        var i, newScale, renderer, sg, xCenterOffset, yCenterOffset;
        g = new dagreD3.graphlib.Graph({
          multigraph: true,
          compound: true
        }).setGraph({
          nodesep: 70,
          edgesep: 0,
          ranksep: 50,
          rankdir: "LR",
          marginx: 40,
          marginy: 40
        });
        loadJsonToDagre(g, data);
        renderer = new dagreD3.render();
        d3mainSvgG.call(renderer, g);
        for (i in subgraphs) {
          sg = subgraphs[i];
          d3mainSvg.select('svg.svg-' + i + ' g').call(renderer, sg);
        }
        newScale = 0.5;
        xCenterOffset = Math.floor((angular.element(mainSvgElement).width() - g.graph().width * newScale) / 2);
        yCenterOffset = Math.floor((angular.element(mainSvgElement).height() - g.graph().height * newScale) / 2);
        mainZoom.scale(newScale).translate([xCenterOffset, yCenterOffset]);
        d3mainSvgG.attr("transform", "translate(" + xCenterOffset + ", " + yCenterOffset + ") scale(" + mainZoom.scale() + ")");
        mainZoom.on("zoom", function() {
          var ev;
          ev = d3.event;
          return d3mainSvgG.attr("transform", "translate(" + ev.translate + ") scale(" + ev.scale + ")");
        });
        mainZoom(d3mainSvg);
        return d3mainSvgG.selectAll('.node').on('click', function(d) {
          return scope.setNode({
            nodeid: d
          });
        });
      };
      scope.$watch(attrs.plan, function(newPlan) {
        if (newPlan) {
          return drawGraph(newPlan);
        }
      });
    }
  };
}]);

angular.module('flinkApp').service('JobsService', ["$http", "flinkConfig", "$log", "amMoment", "$q", "$timeout", function($http, flinkConfig, $log, amMoment, $q, $timeout) {
  var currentJob, currentPlan, deferreds, jobObservers, jobs, notifyObservers;
  currentJob = null;
  currentPlan = null;
  deferreds = {};
  jobs = {
    running: [],
    finished: [],
    cancelled: [],
    failed: []
  };
  jobObservers = [];
  notifyObservers = function() {
    return angular.forEach(jobObservers, function(callback) {
      return callback();
    });
  };
  this.registerObserver = function(callback) {
    return jobObservers.push(callback);
  };
  this.unRegisterObserver = function(callback) {
    var index;
    index = jobObservers.indexOf(callback);
    return jobObservers.splice(index, 1);
  };
  this.stateList = function() {
    return ['SCHEDULED', 'DEPLOYING', 'RUNNING', 'FINISHED', 'FAILED', 'CANCELING', 'CANCELED'];
  };
  this.translateLabelState = function(state) {
    switch (state.toLowerCase()) {
      case 'finished':
        return 'success';
      case 'failed':
        return 'danger';
      case 'scheduled':
        return 'default';
      case 'deploying':
        return 'info';
      case 'running':
        return 'primary';
      case 'canceling':
        return 'warning';
      case 'pending':
        return 'info';
      case 'total':
        return 'black';
      default:
        return 'default';
    }
  };
  this.setEndTimes = function(list) {
    return angular.forEach(list, function(item, jobKey) {
      if (!(item['end-time'] > -1)) {
        return item['end-time'] = item['start-time'] + item['duration'];
      }
    });
  };
  this.processVertices = function(data) {
    angular.forEach(data.vertices, function(vertex, i) {
      return vertex.type = 'regular';
    });
    return data.vertices.unshift({
      name: 'Scheduled',
      'start-time': data.timestamps['CREATED'],
      'end-time': data.timestamps['CREATED'] + 1,
      type: 'scheduled'
    });
  };
  this.listJobs = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "joboverview").success((function(_this) {
      return function(data, status, headers, config) {
        angular.forEach(data, function(list, listKey) {
          switch (listKey) {
            case 'running':
              return jobs.running = _this.setEndTimes(list);
            case 'finished':
              return jobs.finished = _this.setEndTimes(list);
            case 'cancelled':
              return jobs.cancelled = _this.setEndTimes(list);
            case 'failed':
              return jobs.failed = _this.setEndTimes(list);
          }
        });
        deferred.resolve(jobs);
        return notifyObservers();
      };
    })(this));
    return deferred.promise;
  };
  this.getJobs = function(type) {
    return jobs[type];
  };
  this.getAllJobs = function() {
    return jobs;
  };
  this.loadJob = function(jobid) {
    currentJob = null;
    deferreds.job = $q.defer();
    $http.get(flinkConfig.jobServer + "jobs/" + jobid).success((function(_this) {
      return function(data, status, headers, config) {
        _this.setEndTimes(data.vertices);
        _this.processVertices(data);
        return $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/config").success(function(jobConfig) {
          data = angular.extend(data, jobConfig);
          currentJob = data;
          return deferreds.job.resolve(currentJob);
        });
      };
    })(this));
    return deferreds.job.promise;
  };
  this.getNode = function(nodeid) {
    var deferred, seekNode;
    seekNode = function(nodeid, data) {
      var j, len, node, sub;
      for (j = 0, len = data.length; j < len; j++) {
        node = data[j];
        if (node.id === nodeid) {
          return node;
        }
        if (node.step_function) {
          sub = seekNode(nodeid, node.step_function);
        }
        if (sub) {
          return sub;
        }
      }
      return null;
    };
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        var foundNode;
        foundNode = seekNode(nodeid, currentJob.plan.nodes);
        foundNode.vertex = _this.seekVertex(nodeid);
        return deferred.resolve(foundNode);
      };
    })(this));
    return deferred.promise;
  };
  this.seekVertex = function(nodeid) {
    var j, len, ref, vertex;
    ref = currentJob.vertices;
    for (j = 0, len = ref.length; j < len; j++) {
      vertex = ref[j];
      if (vertex.id === nodeid) {
        return vertex;
      }
    }
    return null;
  };
  this.getVertex = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        var vertex;
        vertex = _this.seekVertex(vertexid);
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasktimes").success(function(data) {
          vertex.subtasks = data.subtasks;
          return deferred.resolve(vertex);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getSubtasks = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid).success(function(data) {
          var subtasks;
          subtasks = data.subtasks;
          return deferred.resolve(subtasks);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getTaskManagers = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/taskmanagers").success(function(data) {
          var taskmanagers;
          taskmanagers = data.taskmanagers;
          return deferred.resolve(taskmanagers);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getAccumulators = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        console.log(currentJob.jid);
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/accumulators").success(function(data) {
          var accumulators;
          accumulators = data['user-accumulators'];
          return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasks/accumulators").success(function(data) {
            var subtaskAccumulators;
            subtaskAccumulators = data.subtasks;
            return deferred.resolve({
              main: accumulators,
              subtasks: subtaskAccumulators
            });
          });
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getCheckpointConfig = function() {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/checkpoints/config").success(function(data) {
          if (angular.equals({}, data)) {
            return deferred.resolve(null);
          } else {
            return deferred.resolve(data);
          }
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getCheckpointStats = function() {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/checkpoints").success(function(data, status, headers, config) {
          if (angular.equals({}, data)) {
            return deferred.resolve(null);
          } else {
            return deferred.resolve(data);
          }
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getCheckpointDetails = function(checkpointid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/checkpoints/details/" + checkpointid).success(function(data) {
          if (angular.equals({}, data)) {
            return deferred.resolve(null);
          } else {
            return deferred.resolve(data);
          }
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getCheckpointSubtaskDetails = function(checkpointid, vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/checkpoints/details/" + checkpointid + "/subtasks/" + vertexid).success(function(data) {
          if (angular.equals({}, data)) {
            return deferred.resolve(null);
          } else {
            return deferred.resolve(data);
          }
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getOperatorBackPressure = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/backpressure").success((function(_this) {
      return function(data) {
        return deferred.resolve(data);
      };
    })(this));
    return deferred.promise;
  };
  this.translateBackPressureLabelState = function(state) {
    switch (state.toLowerCase()) {
      case 'in-progress':
        return 'danger';
      case 'ok':
        return 'success';
      case 'low':
        return 'warning';
      case 'high':
        return 'danger';
      default:
        return 'default';
    }
  };
  this.loadExceptions = function() {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/exceptions").success(function(exceptions) {
          currentJob.exceptions = exceptions;
          return deferred.resolve(exceptions);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.cancelJob = function(jobid) {
    return $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/yarn-cancel");
  };
  this.stopJob = function(jobid) {
    return $http.get("jobs/" + jobid + "/yarn-stop");
  };
  return this;
}]);

angular.module('flinkApp').directive('metricsGraph', function() {
  return {
    template: '<div class="panel panel-default panel-metric"> <div class="panel-heading"> <span class="metric-title">{{metric.id}}</span> <div class="buttons"> <div class="btn-group"> <button type="button" ng-class="[btnClasses, {active: metric.size != \'big\'}]" ng-click="setSize(\'small\')">Small</button> <button type="button" ng-class="[btnClasses, {active: metric.size == \'big\'}]" ng-click="setSize(\'big\')">Big</button> </div> <a title="Remove" class="btn btn-default btn-xs remove" ng-click="removeMetric()"><i class="fa fa-close" /></a> </div> </div> <div class="panel-body"> <svg /> </div> </div>',
    replace: true,
    scope: {
      metric: "=",
      window: "=",
      removeMetric: "&",
      setMetricSize: "=",
      getValues: "&"
    },
    link: function(scope, element, attrs) {
      scope.btnClasses = ['btn', 'btn-default', 'btn-xs'];
      scope.value = null;
      scope.data = [
        {
          values: scope.getValues()
        }
      ];
      scope.options = {
        x: function(d, i) {
          return d.x;
        },
        y: function(d, i) {
          return d.y;
        },
        xTickFormat: function(d) {
          return d3.time.format('%H:%M:%S')(new Date(d));
        },
        yTickFormat: function(d) {
          var absD, found, pow, step;
          found = false;
          pow = 0;
          step = 1;
          absD = Math.abs(d);
          while (!found && pow < 50) {
            if (Math.pow(10, pow) <= absD && absD < Math.pow(10, pow + step)) {
              found = true;
            } else {
              pow += step;
            }
          }
          if (found && pow > 6) {
            return (d / Math.pow(10, pow)) + "E" + pow;
          } else {
            return "" + d;
          }
        }
      };
      scope.showChart = function() {
        return d3.select(element.find("svg")[0]).datum(scope.data).transition().duration(250).call(scope.chart);
      };
      scope.chart = nv.models.lineChart().options(scope.options).showLegend(false).margin({
        top: 15,
        left: 60,
        bottom: 30,
        right: 30
      });
      scope.chart.yAxis.showMaxMin(false);
      scope.chart.tooltip.hideDelay(0);
      scope.chart.tooltip.contentGenerator(function(obj) {
        return "<p>" + (d3.time.format('%H:%M:%S')(new Date(obj.point.x))) + " | " + obj.point.y + "</p>";
      });
      nv.utils.windowResize(scope.chart.update);
      scope.setSize = function(size) {
        return scope.setMetricSize(scope.metric, size);
      };
      scope.showChart();
      scope.$on('metrics:data:update', function(event, timestamp, data) {
        scope.value = parseFloat(data[scope.metric.id]);
        scope.data[0].values.push({
          x: timestamp,
          y: scope.value
        });
        if (scope.data[0].values.length > scope.window) {
          scope.data[0].values.shift();
        }
        scope.showChart();
        scope.chart.clearHighlights();
        return scope.chart.tooltip.hidden(true);
      });
      return element.find(".metric-title").qtip({
        content: {
          text: scope.metric.id
        },
        position: {
          my: 'bottom left',
          at: 'top left'
        },
        style: {
          classes: 'qtip-light qtip-timeline-bar'
        }
      });
    }
  };
});

angular.module('flinkApp').service('MetricsService', ["$http", "$q", "flinkConfig", "$interval", function($http, $q, flinkConfig, $interval) {
  this.metrics = {};
  this.values = {};
  this.watched = {};
  this.observer = {
    jobid: null,
    nodeid: null,
    callback: null
  };
  this.refresh = $interval((function(_this) {
    return function() {
      return angular.forEach(_this.metrics, function(vertices, jobid) {
        return angular.forEach(vertices, function(metrics, nodeid) {
          var names;
          names = [];
          angular.forEach(metrics, function(metric, index) {
            return names.push(metric.id);
          });
          if (names.length > 0) {
            return _this.getMetrics(jobid, nodeid, names).then(function(values) {
              if (jobid === _this.observer.jobid && nodeid === _this.observer.nodeid) {
                if (_this.observer.callback) {
                  return _this.observer.callback(values);
                }
              }
            });
          }
        });
      });
    };
  })(this), flinkConfig["refresh-interval"]);
  this.registerObserver = function(jobid, nodeid, callback) {
    this.observer.jobid = jobid;
    this.observer.nodeid = nodeid;
    return this.observer.callback = callback;
  };
  this.unRegisterObserver = function() {
    return this.observer = {
      jobid: null,
      nodeid: null,
      callback: null
    };
  };
  this.setupMetrics = function(jobid, vertices) {
    this.setupLS();
    this.watched[jobid] = [];
    return angular.forEach(vertices, (function(_this) {
      return function(v, k) {
        if (v.id) {
          return _this.watched[jobid].push(v.id);
        }
      };
    })(this));
  };
  this.getWindow = function() {
    return 100;
  };
  this.setupLS = function() {
    if (sessionStorage.flinkMetrics == null) {
      this.saveSetup();
    }
    return this.metrics = JSON.parse(sessionStorage.flinkMetrics);
  };
  this.saveSetup = function() {
    return sessionStorage.flinkMetrics = JSON.stringify(this.metrics);
  };
  this.saveValue = function(jobid, nodeid, value) {
    if (this.values[jobid] == null) {
      this.values[jobid] = {};
    }
    if (this.values[jobid][nodeid] == null) {
      this.values[jobid][nodeid] = [];
    }
    this.values[jobid][nodeid].push(value);
    if (this.values[jobid][nodeid].length > this.getWindow()) {
      return this.values[jobid][nodeid].shift();
    }
  };
  this.getValues = function(jobid, nodeid, metricid) {
    var results;
    if (this.values[jobid] == null) {
      return [];
    }
    if (this.values[jobid][nodeid] == null) {
      return [];
    }
    results = [];
    angular.forEach(this.values[jobid][nodeid], (function(_this) {
      return function(v, k) {
        if (v.values[metricid] != null) {
          return results.push({
            x: v.timestamp,
            y: v.values[metricid]
          });
        }
      };
    })(this));
    return results;
  };
  this.setupLSFor = function(jobid, nodeid) {
    if (this.metrics[jobid] == null) {
      this.metrics[jobid] = {};
    }
    if (this.metrics[jobid][nodeid] == null) {
      return this.metrics[jobid][nodeid] = [];
    }
  };
  this.addMetric = function(jobid, nodeid, metricid) {
    this.setupLSFor(jobid, nodeid);
    this.metrics[jobid][nodeid].push({
      id: metricid,
      size: 'small'
    });
    return this.saveSetup();
  };
  this.removeMetric = (function(_this) {
    return function(jobid, nodeid, metric) {
      var i;
      if (_this.metrics[jobid][nodeid] != null) {
        i = _this.metrics[jobid][nodeid].indexOf(metric);
        if (i === -1) {
          i = _.findIndex(_this.metrics[jobid][nodeid], {
            id: metric
          });
        }
        if (i !== -1) {
          _this.metrics[jobid][nodeid].splice(i, 1);
        }
        return _this.saveSetup();
      }
    };
  })(this);
  this.setMetricSize = (function(_this) {
    return function(jobid, nodeid, metric, size) {
      var i;
      if (_this.metrics[jobid][nodeid] != null) {
        i = _this.metrics[jobid][nodeid].indexOf(metric.id);
        if (i === -1) {
          i = _.findIndex(_this.metrics[jobid][nodeid], {
            id: metric.id
          });
        }
        if (i !== -1) {
          _this.metrics[jobid][nodeid][i] = {
            id: metric.id,
            size: size
          };
        }
        return _this.saveSetup();
      }
    };
  })(this);
  this.orderMetrics = function(jobid, nodeid, item, index) {
    this.setupLSFor(jobid, nodeid);
    angular.forEach(this.metrics[jobid][nodeid], (function(_this) {
      return function(v, k) {
        if (v.id === item.id) {
          _this.metrics[jobid][nodeid].splice(k, 1);
          if (k < index) {
            return index = index - 1;
          }
        }
      };
    })(this));
    this.metrics[jobid][nodeid].splice(index, 0, item);
    return this.saveSetup();
  };
  this.getMetricsSetup = (function(_this) {
    return function(jobid, nodeid) {
      return {
        names: _.map(_this.metrics[jobid][nodeid], function(value) {
          if (_.isString(value)) {
            return {
              id: value,
              size: "small"
            };
          } else {
            return value;
          }
        })
      };
    };
  })(this);
  this.getAvailableMetrics = (function(_this) {
    return function(jobid, nodeid) {
      var deferred;
      _this.setupLSFor(jobid, nodeid);
      deferred = $q.defer();
      $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/vertices/" + nodeid + "/metrics").success(function(data) {
        var results;
        results = [];
        angular.forEach(data, function(v, k) {
          var i;
          i = _this.metrics[jobid][nodeid].indexOf(v.id);
          if (i === -1) {
            i = _.findIndex(_this.metrics[jobid][nodeid], {
              id: v.id
            });
          }
          if (i === -1) {
            return results.push(v);
          }
        });
        return deferred.resolve(results);
      });
      return deferred.promise;
    };
  })(this);
  this.getAllAvailableMetrics = (function(_this) {
    return function(jobid, nodeid) {
      var deferred;
      deferred = $q.defer();
      $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/vertices/" + nodeid + "/metrics").success(function(data) {
        return deferred.resolve(data);
      });
      return deferred.promise;
    };
  })(this);
  this.getMetrics = function(jobid, nodeid, metricIds) {
    var deferred, ids;
    deferred = $q.defer();
    ids = metricIds.join(",");
    $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/vertices/" + nodeid + "/metrics?get=" + ids).success((function(_this) {
      return function(data) {
        var newValue, result;
        result = {};
        angular.forEach(data, function(v, k) {
          return result[v.id] = parseInt(v.value);
        });
        newValue = {
          timestamp: Date.now(),
          values: result
        };
        _this.saveValue(jobid, nodeid, newValue);
        return deferred.resolve(newValue);
      };
    })(this));
    return deferred.promise;
  };
  this.setupLS();
  return this;
}]);

angular.module('flinkApp').controller('OverviewController', ["$scope", "OverviewService", "JobsService", "$interval", "flinkConfig", function($scope, OverviewService, JobsService, $interval, flinkConfig) {
  var refresh;
  $scope.jobObserver = function() {
    $scope.runningJobs = JobsService.getJobs('running');
    return $scope.finishedJobs = JobsService.getJobs('finished');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  $scope.jobObserver();
  OverviewService.loadOverview().then(function(data) {
    return $scope.overview = data;
  });
  refresh = $interval(function() {
    return OverviewService.loadOverview().then(function(data) {
      return $scope.overview = data;
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]);

angular.module('flinkApp').service('OverviewService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var overview;
  overview = {};
  this.loadOverview = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "overview").success(function(data, status, headers, config) {
      overview = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('JobSubmitController', ["$scope", "JobSubmitService", "$interval", "flinkConfig", "$state", "$location", function($scope, JobSubmitService, $interval, flinkConfig, $state, $location) {
  var refresh;
  $scope.yarn = $location.absUrl().indexOf("/proxy/application_") !== -1;
  $scope.loadList = function() {
    return JobSubmitService.loadJarList().then(function(data) {
      $scope.address = data.address;
      $scope.noaccess = data.error;
      return $scope.jars = data.files;
    });
  };
  $scope.defaultState = function() {
    $scope.plan = null;
    $scope.error = null;
    return $scope.state = {
      selected: null,
      parallelism: "",
      savepointPath: "",
      allowNonRestoredState: false,
      'entry-class': "",
      'program-args': "",
      'plan-button': "Show Plan",
      'submit-button': "Submit",
      'action-time': 0
    };
  };
  $scope.defaultState();
  $scope.uploader = {};
  $scope.loadList();
  refresh = $interval(function() {
    return $scope.loadList();
  }, flinkConfig["refresh-interval"]);
  $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
  $scope.selectJar = function(id) {
    if ($scope.state.selected === id) {
      return $scope.defaultState();
    } else {
      $scope.defaultState();
      return $scope.state.selected = id;
    }
  };
  $scope.deleteJar = function(event, id) {
    if ($scope.state.selected === id) {
      $scope.defaultState();
    }
    angular.element(event.currentTarget).removeClass("fa-remove").addClass("fa-spin fa-spinner");
    return JobSubmitService.deleteJar(id).then(function(data) {
      angular.element(event.currentTarget).removeClass("fa-spin fa-spinner").addClass("fa-remove");
      if (data.error != null) {
        return alert(data.error);
      }
    });
  };
  $scope.loadEntryClass = function(name) {
    return $scope.state['entry-class'] = name;
  };
  $scope.getPlan = function() {
    var action;
    if ($scope.state['plan-button'] === "Show Plan") {
      action = new Date().getTime();
      $scope.state['action-time'] = action;
      $scope.state['submit-button'] = "Submit";
      $scope.state['plan-button'] = "Getting Plan";
      $scope.error = null;
      $scope.plan = null;
      return JobSubmitService.getPlan($scope.state.selected, {
        'entry-class': $scope.state['entry-class'],
        parallelism: $scope.state.parallelism,
        'program-args': $scope.state['program-args']
      }).then(function(data) {
        if (action === $scope.state['action-time']) {
          $scope.state['plan-button'] = "Show Plan";
          $scope.error = data.error;
          return $scope.plan = data.plan;
        }
      });
    }
  };
  $scope.runJob = function() {
    var action;
    if ($scope.state['submit-button'] === "Submit") {
      action = new Date().getTime();
      $scope.state['action-time'] = action;
      $scope.state['submit-button'] = "Submitting";
      $scope.state['plan-button'] = "Show Plan";
      $scope.error = null;
      return JobSubmitService.runJob($scope.state.selected, {
        'entry-class': $scope.state['entry-class'],
        parallelism: $scope.state.parallelism,
        'program-args': $scope.state['program-args'],
        savepointPath: $scope.state['savepointPath'],
        allowNonRestoredState: $scope.state['allowNonRestoredState']
      }).then(function(data) {
        if (action === $scope.state['action-time']) {
          $scope.state['submit-button'] = "Submit";
          $scope.error = data.error;
          if (data.jobid != null) {
            return $state.go("single-job.plan.subtasks", {
              jobid: data.jobid
            });
          }
        }
      });
    }
  };
  $scope.nodeid = null;
  $scope.changeNode = function(nodeid) {
    if (nodeid !== $scope.nodeid) {
      $scope.nodeid = nodeid;
      $scope.vertex = null;
      $scope.subtasks = null;
      $scope.accumulators = null;
      return $scope.$broadcast('reload');
    } else {
      $scope.nodeid = null;
      $scope.nodeUnfolded = false;
      $scope.vertex = null;
      $scope.subtasks = null;
      return $scope.accumulators = null;
    }
  };
  $scope.clearFiles = function() {
    return $scope.uploader = {};
  };
  $scope.uploadFiles = function(files) {
    $scope.uploader = {};
    if (files.length === 1) {
      $scope.uploader['file'] = files[0];
      return $scope.uploader['upload'] = true;
    } else {
      return $scope.uploader['error'] = "Did ya forget to select a file?";
    }
  };
  return $scope.startUpload = function() {
    var formdata, xhr;
    if ($scope.uploader['file'] != null) {
      formdata = new FormData();
      formdata.append("jarfile", $scope.uploader['file']);
      $scope.uploader['upload'] = false;
      $scope.uploader['success'] = "Initializing upload...";
      xhr = new XMLHttpRequest();
      xhr.upload.onprogress = function(event) {
        $scope.uploader['success'] = null;
        return $scope.uploader['progress'] = parseInt(100 * event.loaded / event.total);
      };
      xhr.upload.onerror = function(event) {
        $scope.uploader['progress'] = null;
        return $scope.uploader['error'] = "An error occurred while uploading your file";
      };
      xhr.upload.onload = function(event) {
        $scope.uploader['progress'] = null;
        return $scope.uploader['success'] = "Saving...";
      };
      xhr.onreadystatechange = function() {
        var response;
        if (xhr.readyState === 4) {
          response = JSON.parse(xhr.responseText);
          if (response.error != null) {
            $scope.uploader['error'] = response.error;
            return $scope.uploader['success'] = null;
          } else {
            return $scope.uploader['success'] = "Uploaded!";
          }
        }
      };
      xhr.open("POST", "/jars/upload");
      return xhr.send(formdata);
    } else {
      return console.log("Unexpected Error. This should not happen");
    }
  };
}]).filter('getJarSelectClass', function() {
  return function(selected, actual) {
    if (selected === actual) {
      return "fa-check-square";
    } else {
      return "fa-square-o";
    }
  };
});

angular.module('flinkApp').service('JobSubmitService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadJarList = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jars/").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.deleteJar = function(id) {
    var deferred;
    deferred = $q.defer();
    $http["delete"](flinkConfig.jobServer + "jars/" + encodeURIComponent(id)).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.getPlan = function(id, args) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jars/" + encodeURIComponent(id) + "/plan", {
      params: args
    }).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.runJob = function(id, args) {
    var deferred;
    deferred = $q.defer();
    $http.post(flinkConfig.jobServer + "jars/" + encodeURIComponent(id) + "/run", {}, {
      params: args
    }).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('AllTaskManagersController', ["$scope", "TaskManagersService", "$interval", "flinkConfig", function($scope, TaskManagersService, $interval, flinkConfig) {
  var refresh;
  TaskManagersService.loadManagers().then(function(data) {
    return $scope.managers = data;
  });
  refresh = $interval(function() {
    return TaskManagersService.loadManagers().then(function(data) {
      return $scope.managers = data;
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]).controller('SingleTaskManagerController', ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) {
  var refresh;
  $scope.metrics = {};
  SingleTaskManagerService.loadMetrics($stateParams.taskmanagerid).then(function(data) {
    return $scope.metrics = data[0];
  });
  refresh = $interval(function() {
    return SingleTaskManagerService.loadMetrics($stateParams.taskmanagerid).then(function(data) {
      return $scope.metrics = data[0];
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]).controller('SingleTaskManagerLogsController', ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) {
  $scope.log = {};
  $scope.taskmanagerid = $stateParams.taskmanagerid;
  SingleTaskManagerService.loadLogs($stateParams.taskmanagerid).then(function(data) {
    return $scope.log = data;
  });
  return $scope.reloadData = function() {
    return SingleTaskManagerService.loadLogs($stateParams.taskmanagerid).then(function(data) {
      return $scope.log = data;
    });
  };
}]).controller('SingleTaskManagerStdoutController', ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) {
  $scope.stdout = {};
  $scope.taskmanagerid = $stateParams.taskmanagerid;
  SingleTaskManagerService.loadStdout($stateParams.taskmanagerid).then(function(data) {
    return $scope.stdout = data;
  });
  return $scope.reloadData = function() {
    return SingleTaskManagerService.loadStdout($stateParams.taskmanagerid).then(function(data) {
      return $scope.stdout = data;
    });
  };
}]);

angular.module('flinkApp').service('TaskManagersService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadManagers = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "taskmanagers").success(function(data, status, headers, config) {
      return deferred.resolve(data['taskmanagers']);
    });
    return deferred.promise;
  };
  return this;
}]).service('SingleTaskManagerService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadMetrics = function(taskmanagerid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "taskmanagers/" + taskmanagerid + "/metrics").success(function(data, status, headers, config) {
      return deferred.resolve(data['taskmanagers']);
    });
    return deferred.promise;
  };
  this.loadLogs = function(taskmanagerid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "taskmanagers/" + taskmanagerid + "/log").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.loadStdout = function(taskmanagerid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "taskmanagers/" + taskmanagerid + "/stdout").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsImNvbW1vbi9zZXJ2aWNlcy5jb2ZmZWUiLCJjb21tb24vc2VydmljZXMuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvam9ibWFuYWdlci9qb2JtYW5hZ2VyLmN0cmwuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5zdmMuY29mZmVlIiwibW9kdWxlcy9qb2JtYW5hZ2VyL2pvYm1hbmFnZXIuc3ZjLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5jdHJsLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuZGlyLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLmRpci5qcyIsIm1vZHVsZXMvam9icy9qb2JzLnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5zdmMuanMiLCJtb2R1bGVzL2pvYnMvbWV0cmljcy5kaXIuY29mZmVlIiwibW9kdWxlcy9qb2JzL21ldHJpY3MuZGlyLmpzIiwibW9kdWxlcy9qb2JzL21ldHJpY3Muc3ZjLmNvZmZlZSIsIm1vZHVsZXMvam9icy9tZXRyaWNzLnN2Yy5qcyIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LmN0cmwuanMiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LnN2Yy5qcyIsIm1vZHVsZXMvc3VibWl0L3N1Ym1pdC5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvc3VibWl0L3N1Ym1pdC5jdHJsLmpzIiwibW9kdWxlcy9zdWJtaXQvc3VibWl0LnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL3N1Ym1pdC9zdWJtaXQuc3ZjLmpzIiwibW9kdWxlcy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuY3RybC5qcyIsIm1vZHVsZXMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuc3ZjLmNvZmZlZSIsIm1vZHVsZXMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuc3ZjLmpzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQWtCQSxRQUFRLE9BQU8sWUFBWSxDQUFDLGFBQWEsaUJBQWlCLGFBSXpELG1CQUFJLFNBQUMsWUFBRDtFQUNILFdBQVcsaUJBQWlCO0VDckI1QixPRHNCQSxXQUFXLGNBQWMsV0FBQTtJQUN2QixXQUFXLGlCQUFpQixDQUFDLFdBQVc7SUNyQnhDLE9Ec0JBLFdBQVcsZUFBZTs7SUFJN0IsTUFBTSxlQUFlO0VBQ3BCLFdBQVc7RUFFWCxvQkFBb0I7R0FLckIsK0RBQUksU0FBQyxhQUFhLGFBQWEsYUFBYSxXQUF4QztFQzVCSCxPRDZCQSxZQUFZLGFBQWEsS0FBSyxTQUFDLFFBQUQ7SUFDNUIsUUFBUSxPQUFPLGFBQWE7SUFFNUIsWUFBWTtJQzdCWixPRCtCQSxVQUFVLFdBQUE7TUM5QlIsT0QrQkEsWUFBWTtPQUNaLFlBQVk7O0lBS2pCLGlDQUFPLFNBQUMsdUJBQUQ7RUNqQ04sT0RrQ0Esc0JBQXNCO0lBSXZCLDZCQUFJLFNBQUMsWUFBWSxRQUFiO0VDcENILE9EcUNBLFdBQVcsSUFBSSxxQkFBcUIsU0FBQyxPQUFPLFNBQVMsVUFBVSxXQUEzQjtJQUNsQyxJQUFHLFFBQVEsWUFBWDtNQUNFLE1BQU07TUNwQ04sT0RxQ0EsT0FBTyxHQUFHLFFBQVEsWUFBWTs7O0lBSW5DLGdEQUFPLFNBQUMsZ0JBQWdCLG9CQUFqQjtFQUNOLGVBQWUsTUFBTSxZQUNuQjtJQUFBLEtBQUs7SUFDTCxPQUNFO01BQUEsTUFDRTtRQUFBLGFBQWE7UUFDYixZQUFZOzs7S0FFakIsTUFBTSxn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T0R3QkEsU0FBUyxRQUFROztJQ3RCbkIsT0R3QkEsU0FBUzs7RUFFWCxLQUFDLGFBQWEsU0FBQyxlQUFEO0lBQ1osSUFBQTtJQUFBLFdBQVcsR0FBRztJQUVkLE1BQU0sSUFBSSxZQUFZLFlBQVksa0JBQWtCLGdCQUFnQixXQUNuRSxRQUFRLFNBQUMsTUFBTSxRQUFRLFNBQVMsUUFBeEI7TUN4QlAsT0R5QkEsU0FBUyxRQUFROztJQ3ZCbkIsT0R5QkEsU0FBUzs7RUN2QlgsT0R5QkE7O0FDdkJGIiwiZmlsZSI6ImluZGV4LmpzIiwic291cmNlc0NvbnRlbnQiOlsiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcsIFsndWkucm91dGVyJywgJ2FuZ3VsYXJNb21lbnQnLCAnZG5kTGlzdHMnXSlcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5ydW4gKCRyb290U2NvcGUpIC0+XHJcbiAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9IGZhbHNlXHJcbiAgJHJvb3RTY29wZS5zaG93U2lkZWJhciA9IC0+XHJcbiAgICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gISRyb290U2NvcGUuc2lkZWJhclZpc2libGVcclxuICAgICRyb290U2NvcGUuc2lkZWJhckNsYXNzID0gJ2ZvcmNlLXNob3cnXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4udmFsdWUgJ2ZsaW5rQ29uZmlnJywge1xyXG4gIGpvYlNlcnZlcjogJydcclxuIyAgam9iU2VydmVyOiAnaHR0cDovL2xvY2FsaG9zdDo4MDgxLydcclxuICBcInJlZnJlc2gtaW50ZXJ2YWxcIjogMTAwMDBcclxufVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLnJ1biAoSm9ic1NlcnZpY2UsIE1haW5TZXJ2aWNlLCBmbGlua0NvbmZpZywgJGludGVydmFsKSAtPlxyXG4gIE1haW5TZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuIChjb25maWcpIC0+XHJcbiAgICBhbmd1bGFyLmV4dGVuZCBmbGlua0NvbmZpZywgY29uZmlnXHJcblxyXG4gICAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKVxyXG5cclxuICAgICRpbnRlcnZhbCAtPlxyXG4gICAgICBKb2JzU2VydmljZS5saXN0Sm9icygpXHJcbiAgICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxyXG5cclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5jb25maWcgKCR1aVZpZXdTY3JvbGxQcm92aWRlcikgLT5cclxuICAkdWlWaWV3U2Nyb2xsUHJvdmlkZXIudXNlQW5jaG9yU2Nyb2xsKClcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5ydW4gKCRyb290U2NvcGUsICRzdGF0ZSkgLT5cclxuICAkcm9vdFNjb3BlLiRvbiAnJHN0YXRlQ2hhbmdlU3RhcnQnLCAoZXZlbnQsIHRvU3RhdGUsIHRvUGFyYW1zLCBmcm9tU3RhdGUpIC0+XHJcbiAgICBpZiB0b1N0YXRlLnJlZGlyZWN0VG9cclxuICAgICAgZXZlbnQucHJldmVudERlZmF1bHQoKVxyXG4gICAgICAkc3RhdGUuZ28gdG9TdGF0ZS5yZWRpcmVjdFRvLCB0b1BhcmFtc1xyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbmZpZyAoJHN0YXRlUHJvdmlkZXIsICR1cmxSb3V0ZXJQcm92aWRlcikgLT5cclxuICAkc3RhdGVQcm92aWRlci5zdGF0ZSBcIm92ZXJ2aWV3XCIsXHJcbiAgICB1cmw6IFwiL292ZXJ2aWV3XCJcclxuICAgIHZpZXdzOlxyXG4gICAgICBtYWluOlxyXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL292ZXJ2aWV3Lmh0bWxcIlxyXG4gICAgICAgIGNvbnRyb2xsZXI6ICdPdmVydmlld0NvbnRyb2xsZXInXHJcblxyXG4gIC5zdGF0ZSBcInJ1bm5pbmctam9ic1wiLFxyXG4gICAgdXJsOiBcIi9ydW5uaW5nLWpvYnNcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIG1haW46XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9ydW5uaW5nLWpvYnMuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcclxuXHJcbiAgLnN0YXRlIFwiY29tcGxldGVkLWpvYnNcIixcclxuICAgIHVybDogXCIvY29tcGxldGVkLWpvYnNcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIG1haW46XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9jb21wbGV0ZWQtam9icy5odG1sXCJcclxuICAgICAgICBjb250cm9sbGVyOiAnQ29tcGxldGVkSm9ic0NvbnRyb2xsZXInXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2JcIixcclxuICAgIHVybDogXCIvam9icy97am9iaWR9XCJcclxuICAgIGFic3RyYWN0OiB0cnVlXHJcbiAgICB2aWV3czpcclxuICAgICAgbWFpbjpcclxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5odG1sXCJcclxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlSm9iQ29udHJvbGxlcidcclxuXHJcbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuXCIsXHJcbiAgICB1cmw6IFwiXCJcclxuICAgIHJlZGlyZWN0VG86IFwic2luZ2xlLWpvYi5wbGFuLnN1YnRhc2tzXCJcclxuICAgIHZpZXdzOlxyXG4gICAgICBkZXRhaWxzOlxyXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4uaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5Db250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uc3VidGFza3NcIixcclxuICAgIHVybDogXCJcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgICdub2RlLWRldGFpbHMnOlxyXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LnN1YnRhc2tzLmh0bWxcIlxyXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuU3VidGFza3NDb250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4ubWV0cmljc1wiLFxyXG4gICAgdXJsOiBcIi9tZXRyaWNzXCJcclxuICAgIHZpZXdzOlxyXG4gICAgICAnbm9kZS1kZXRhaWxzJzpcclxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5tZXRyaWNzLmh0bWxcIlxyXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuTWV0cmljc0NvbnRyb2xsZXInXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi50YXNrbWFuYWdlcnNcIixcclxuICAgIHVybDogXCIvdGFza21hbmFnZXJzXCJcclxuICAgIHZpZXdzOlxyXG4gICAgICAnbm9kZS1kZXRhaWxzJzpcclxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC50YXNrbWFuYWdlcnMuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5UYXNrTWFuYWdlcnNDb250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uYWNjdW11bGF0b3JzXCIsXHJcbiAgICB1cmw6IFwiL2FjY3VtdWxhdG9yc1wiXHJcbiAgICB2aWV3czpcclxuICAgICAgJ25vZGUtZGV0YWlscyc6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLWxpc3QuYWNjdW11bGF0b3JzLmh0bWxcIlxyXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcidcclxuXHJcbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzXCIsXHJcbiAgICB1cmw6IFwiL2NoZWNrcG9pbnRzXCJcclxuICAgIHJlZGlyZWN0VG86IFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzLm92ZXJ2aWV3XCJcclxuICAgIHZpZXdzOlxyXG4gICAgICAnbm9kZS1kZXRhaWxzJzpcclxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5jaGVja3BvaW50cy5odG1sXCJcclxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcclxuXHJcbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzLm92ZXJ2aWV3XCIsXHJcbiAgICB1cmw6IFwiL292ZXJ2aWV3XCJcclxuICAgIHZpZXdzOlxyXG4gICAgICAnY2hlY2twb2ludHMtdmlldyc6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLmNoZWNrcG9pbnRzLm92ZXJ2aWV3Lmh0bWxcIlxyXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMuc3VtbWFyeVwiLFxyXG4gICAgdXJsOiBcIi9zdW1tYXJ5XCJcclxuICAgIHZpZXdzOlxyXG4gICAgICAnY2hlY2twb2ludHMtdmlldyc6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLmNoZWNrcG9pbnRzLnN1bW1hcnkuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5jaGVja3BvaW50cy5oaXN0b3J5XCIsXHJcbiAgICB1cmw6IFwiL2hpc3RvcnlcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgICdjaGVja3BvaW50cy12aWV3JzpcclxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuY2hlY2twb2ludHMuaGlzdG9yeS5odG1sXCJcclxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcclxuXHJcbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzLmNvbmZpZ1wiLFxyXG4gICAgdXJsOiBcIi9jb25maWdcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgICdjaGVja3BvaW50cy12aWV3JzpcclxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuY2hlY2twb2ludHMuY29uZmlnLmh0bWxcIlxyXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMuZGV0YWlsc1wiLFxyXG4gICAgdXJsOiBcIi9kZXRhaWxzL3tjaGVja3BvaW50SWR9XCJcclxuICAgIHZpZXdzOlxyXG4gICAgICAnY2hlY2twb2ludHMtdmlldyc6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLmNoZWNrcG9pbnRzLmRldGFpbHMuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5DaGVja3BvaW50RGV0YWlsc0NvbnRyb2xsZXInXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5iYWNrcHJlc3N1cmVcIixcclxuICAgIHVybDogXCIvYmFja3ByZXNzdXJlXCJcclxuICAgIHZpZXdzOlxyXG4gICAgICAnbm9kZS1kZXRhaWxzJzpcclxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5iYWNrcHJlc3N1cmUuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnRpbWVsaW5lXCIsXHJcbiAgICB1cmw6IFwiL3RpbWVsaW5lXCJcclxuICAgIHZpZXdzOlxyXG4gICAgICBkZXRhaWxzOlxyXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLmh0bWxcIlxyXG5cclxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLFxyXG4gICAgdXJsOiBcIi97dmVydGV4SWR9XCJcclxuICAgIHZpZXdzOlxyXG4gICAgICB2ZXJ0ZXg6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUudmVydGV4Lmh0bWxcIlxyXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IuZXhjZXB0aW9uc1wiLFxyXG4gICAgdXJsOiBcIi9leGNlcHRpb25zXCJcclxuICAgIHZpZXdzOlxyXG4gICAgICBkZXRhaWxzOlxyXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmV4Y2VwdGlvbnMuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJzaW5nbGUtam9iLmNvbmZpZ1wiLFxyXG4gICAgdXJsOiBcIi9jb25maWdcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIGRldGFpbHM6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuY29uZmlnLmh0bWxcIlxyXG5cclxuICAuc3RhdGUgXCJhbGwtbWFuYWdlclwiLFxyXG4gICAgdXJsOiBcIi90YXNrbWFuYWdlcnNcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIG1haW46XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXIvaW5kZXguaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0FsbFRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1tYW5hZ2VyXCIsXHJcbiAgICAgIHVybDogXCIvdGFza21hbmFnZXIve3Rhc2ttYW5hZ2VyaWR9XCJcclxuICAgICAgYWJzdHJhY3Q6IHRydWVcclxuICAgICAgdmlld3M6XHJcbiAgICAgICAgbWFpbjpcclxuICAgICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLmh0bWxcIlxyXG4gICAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZVRhc2tNYW5hZ2VyQ29udHJvbGxlcidcclxuXHJcbiAgLnN0YXRlIFwic2luZ2xlLW1hbmFnZXIubWV0cmljc1wiLFxyXG4gICAgdXJsOiBcIi9tZXRyaWNzXCJcclxuICAgIHZpZXdzOlxyXG4gICAgICBkZXRhaWxzOlxyXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLm1ldHJpY3MuaHRtbFwiXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1tYW5hZ2VyLnN0ZG91dFwiLFxyXG4gICAgdXJsOiBcIi9zdGRvdXRcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIGRldGFpbHM6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuc3Rkb3V0Lmh0bWxcIlxyXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVUYXNrTWFuYWdlclN0ZG91dENvbnRyb2xsZXInXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1tYW5hZ2VyLmxvZ1wiLFxyXG4gICAgdXJsOiBcIi9sb2dcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIGRldGFpbHM6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXIvdGFza21hbmFnZXIubG9nLmh0bWxcIlxyXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVUYXNrTWFuYWdlckxvZ3NDb250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJqb2JtYW5hZ2VyXCIsXHJcbiAgICAgIHVybDogXCIvam9ibWFuYWdlclwiXHJcbiAgICAgIHZpZXdzOlxyXG4gICAgICAgIG1haW46XHJcbiAgICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2luZGV4Lmh0bWxcIlxyXG5cclxuICAuc3RhdGUgXCJqb2JtYW5hZ2VyLmNvbmZpZ1wiLFxyXG4gICAgdXJsOiBcIi9jb25maWdcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIGRldGFpbHM6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9jb25maWcuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJqb2JtYW5hZ2VyLnN0ZG91dFwiLFxyXG4gICAgdXJsOiBcIi9zdGRvdXRcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIGRldGFpbHM6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9zdGRvdXQuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJTdGRvdXRDb250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJqb2JtYW5hZ2VyLmxvZ1wiLFxyXG4gICAgdXJsOiBcIi9sb2dcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIGRldGFpbHM6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9sb2cuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJMb2dzQ29udHJvbGxlcidcclxuXHJcbiAgLnN0YXRlIFwic3VibWl0XCIsXHJcbiAgICAgIHVybDogXCIvc3VibWl0XCJcclxuICAgICAgdmlld3M6XHJcbiAgICAgICAgbWFpbjpcclxuICAgICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3N1Ym1pdC5odG1sXCJcclxuICAgICAgICAgIGNvbnRyb2xsZXI6IFwiSm9iU3VibWl0Q29udHJvbGxlclwiXHJcblxyXG4gICR1cmxSb3V0ZXJQcm92aWRlci5vdGhlcndpc2UgXCIvb3ZlcnZpZXdcIlxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnLCBbJ3VpLnJvdXRlcicsICdhbmd1bGFyTW9tZW50JywgJ2RuZExpc3RzJ10pLnJ1bihmdW5jdGlvbigkcm9vdFNjb3BlKSB7XG4gICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSBmYWxzZTtcbiAgcmV0dXJuICRyb290U2NvcGUuc2hvd1NpZGViYXIgPSBmdW5jdGlvbigpIHtcbiAgICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gISRyb290U2NvcGUuc2lkZWJhclZpc2libGU7XG4gICAgcmV0dXJuICRyb290U2NvcGUuc2lkZWJhckNsYXNzID0gJ2ZvcmNlLXNob3cnO1xuICB9O1xufSkudmFsdWUoJ2ZsaW5rQ29uZmlnJywge1xuICBqb2JTZXJ2ZXI6ICcnLFxuICBcInJlZnJlc2gtaW50ZXJ2YWxcIjogMTAwMDBcbn0pLnJ1bihmdW5jdGlvbihKb2JzU2VydmljZSwgTWFpblNlcnZpY2UsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIHtcbiAgcmV0dXJuIE1haW5TZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuKGZ1bmN0aW9uKGNvbmZpZykge1xuICAgIGFuZ3VsYXIuZXh0ZW5kKGZsaW5rQ29uZmlnLCBjb25maWcpO1xuICAgIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKCk7XG4gICAgcmV0dXJuICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5saXN0Sm9icygpO1xuICAgIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIH0pO1xufSkuY29uZmlnKGZ1bmN0aW9uKCR1aVZpZXdTY3JvbGxQcm92aWRlcikge1xuICByZXR1cm4gJHVpVmlld1Njcm9sbFByb3ZpZGVyLnVzZUFuY2hvclNjcm9sbCgpO1xufSkucnVuKGZ1bmN0aW9uKCRyb290U2NvcGUsICRzdGF0ZSkge1xuICByZXR1cm4gJHJvb3RTY29wZS4kb24oJyRzdGF0ZUNoYW5nZVN0YXJ0JywgZnVuY3Rpb24oZXZlbnQsIHRvU3RhdGUsIHRvUGFyYW1zLCBmcm9tU3RhdGUpIHtcbiAgICBpZiAodG9TdGF0ZS5yZWRpcmVjdFRvKSB7XG4gICAgICBldmVudC5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgcmV0dXJuICRzdGF0ZS5nbyh0b1N0YXRlLnJlZGlyZWN0VG8sIHRvUGFyYW1zKTtcbiAgICB9XG4gIH0pO1xufSkuY29uZmlnKGZ1bmN0aW9uKCRzdGF0ZVByb3ZpZGVyLCAkdXJsUm91dGVyUHJvdmlkZXIpIHtcbiAgJHN0YXRlUHJvdmlkZXIuc3RhdGUoXCJvdmVydmlld1wiLCB7XG4gICAgdXJsOiBcIi9vdmVydmlld1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL292ZXJ2aWV3Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwicnVubmluZy1qb2JzXCIsIHtcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvcnVubmluZy1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiY29tcGxldGVkLWpvYnNcIiwge1xuICAgIHVybDogXCIvY29tcGxldGVkLWpvYnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2NvbXBsZXRlZC1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iXCIsIHtcbiAgICB1cmw6IFwiL2pvYnMve2pvYmlkfVwiLFxuICAgIGFic3RyYWN0OiB0cnVlLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhblwiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIHJlZGlyZWN0VG86IFwic2luZ2xlLWpvYi5wbGFuLnN1YnRhc2tzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLnN1YnRhc2tzXCIsIHtcbiAgICB1cmw6IFwiXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LnN1YnRhc2tzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5TdWJ0YXNrc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5tZXRyaWNzXCIsIHtcbiAgICB1cmw6IFwiL21ldHJpY3NcIixcbiAgICB2aWV3czoge1xuICAgICAgJ25vZGUtZGV0YWlscyc6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLWxpc3QubWV0cmljcy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuTWV0cmljc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi50YXNrbWFuYWdlcnNcIiwge1xuICAgIHVybDogXCIvdGFza21hbmFnZXJzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LnRhc2ttYW5hZ2Vycy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuVGFza01hbmFnZXJzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLmFjY3VtdWxhdG9yc1wiLCB7XG4gICAgdXJsOiBcIi9hY2N1bXVsYXRvcnNcIixcbiAgICB2aWV3czoge1xuICAgICAgJ25vZGUtZGV0YWlscyc6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLWxpc3QuYWNjdW11bGF0b3JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHNcIiwge1xuICAgIHVybDogXCIvY2hlY2twb2ludHNcIixcbiAgICByZWRpcmVjdFRvOiBcInNpbmdsZS1qb2IucGxhbi5jaGVja3BvaW50cy5vdmVydmlld1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5jaGVja3BvaW50cy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMub3ZlcnZpZXdcIiwge1xuICAgIHVybDogXCIvb3ZlcnZpZXdcIixcbiAgICB2aWV3czoge1xuICAgICAgJ2NoZWNrcG9pbnRzLXZpZXcnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5jaGVja3BvaW50cy5vdmVydmlldy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMuc3VtbWFyeVwiLCB7XG4gICAgdXJsOiBcIi9zdW1tYXJ5XCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdjaGVja3BvaW50cy12aWV3Jzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuY2hlY2twb2ludHMuc3VtbWFyeS5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMuaGlzdG9yeVwiLCB7XG4gICAgdXJsOiBcIi9oaXN0b3J5XCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdjaGVja3BvaW50cy12aWV3Jzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuY2hlY2twb2ludHMuaGlzdG9yeS5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMuY29uZmlnXCIsIHtcbiAgICB1cmw6IFwiL2NvbmZpZ1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnY2hlY2twb2ludHMtdmlldyc6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLmNoZWNrcG9pbnRzLmNvbmZpZy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMuZGV0YWlsc1wiLCB7XG4gICAgdXJsOiBcIi9kZXRhaWxzL3tjaGVja3BvaW50SWR9XCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdjaGVja3BvaW50cy12aWV3Jzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuY2hlY2twb2ludHMuZGV0YWlscy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ2hlY2twb2ludERldGFpbHNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4uYmFja3ByZXNzdXJlXCIsIHtcbiAgICB1cmw6IFwiL2JhY2twcmVzc3VyZVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5iYWNrcHJlc3N1cmUuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkJhY2tQcmVzc3VyZUNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IudGltZWxpbmVcIiwge1xuICAgIHVybDogXCIvdGltZWxpbmVcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIiwge1xuICAgIHVybDogXCIve3ZlcnRleElkfVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICB2ZXJ0ZXg6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUudmVydGV4Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5leGNlcHRpb25zXCIsIHtcbiAgICB1cmw6IFwiL2V4Y2VwdGlvbnNcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5leGNlcHRpb25zLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLmNvbmZpZ1wiLCB7XG4gICAgdXJsOiBcIi9jb25maWdcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5jb25maWcuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImFsbC1tYW5hZ2VyXCIsIHtcbiAgICB1cmw6IFwiL3Rhc2ttYW5hZ2Vyc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL2luZGV4Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0FsbFRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1tYW5hZ2VyXCIsIHtcbiAgICB1cmw6IFwiL3Rhc2ttYW5hZ2VyL3t0YXNrbWFuYWdlcmlkfVwiLFxuICAgIGFic3RyYWN0OiB0cnVlLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZVRhc2tNYW5hZ2VyQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLW1hbmFnZXIubWV0cmljc1wiLCB7XG4gICAgdXJsOiBcIi9tZXRyaWNzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXIvdGFza21hbmFnZXIubWV0cmljcy5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLW1hbmFnZXIuc3Rkb3V0XCIsIHtcbiAgICB1cmw6IFwiL3N0ZG91dFwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLnN0ZG91dC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVUYXNrTWFuYWdlclN0ZG91dENvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1tYW5hZ2VyLmxvZ1wiLCB7XG4gICAgdXJsOiBcIi9sb2dcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5sb2cuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlVGFza01hbmFnZXJMb2dzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiam9ibWFuYWdlclwiLCB7XG4gICAgdXJsOiBcIi9qb2JtYW5hZ2VyXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9pbmRleC5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiam9ibWFuYWdlci5jb25maWdcIiwge1xuICAgIHVybDogXCIvY29uZmlnXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9jb25maWcuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iTWFuYWdlckNvbmZpZ0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImpvYm1hbmFnZXIuc3Rkb3V0XCIsIHtcbiAgICB1cmw6IFwiL3N0ZG91dFwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvc3Rkb3V0Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJTdGRvdXRDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJqb2JtYW5hZ2VyLmxvZ1wiLCB7XG4gICAgdXJsOiBcIi9sb2dcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2xvZy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyTG9nc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInN1Ym1pdFwiLCB7XG4gICAgdXJsOiBcIi9zdWJtaXRcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9zdWJtaXQuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiBcIkpvYlN1Ym1pdENvbnRyb2xsZXJcIlxuICAgICAgfVxuICAgIH1cbiAgfSk7XG4gIHJldHVybiAkdXJsUm91dGVyUHJvdmlkZXIub3RoZXJ3aXNlKFwiL292ZXJ2aWV3XCIpO1xufSk7XG4iLCIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmRpcmVjdGl2ZSAnYnNMYWJlbCcsIChKb2JzU2VydmljZSkgLT5cclxuICB0cmFuc2NsdWRlOiB0cnVlXHJcbiAgcmVwbGFjZTogdHJ1ZVxyXG4gIHNjb3BlOiBcclxuICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiXHJcbiAgICBzdGF0dXM6IFwiQFwiXHJcblxyXG4gIHRlbXBsYXRlOiBcIjxzcGFuIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJz48bmctdHJhbnNjbHVkZT48L25nLXRyYW5zY2x1ZGU+PC9zcGFuPlwiXHJcbiAgXHJcbiAgbGluazogKHNjb3BlLCBlbGVtZW50LCBhdHRycykgLT5cclxuICAgIHNjb3BlLmdldExhYmVsQ2xhc3MgPSAtPlxyXG4gICAgICAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uZGlyZWN0aXZlICdicExhYmVsJywgKEpvYnNTZXJ2aWNlKSAtPlxyXG4gIHRyYW5zY2x1ZGU6IHRydWVcclxuICByZXBsYWNlOiB0cnVlXHJcbiAgc2NvcGU6XHJcbiAgICBnZXRCYWNrUHJlc3N1cmVMYWJlbENsYXNzOiBcIiZcIlxyXG4gICAgc3RhdHVzOiBcIkBcIlxyXG5cclxuICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldEJhY2tQcmVzc3VyZUxhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIlxyXG5cclxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxyXG4gICAgc2NvcGUuZ2V0QmFja1ByZXNzdXJlTGFiZWxDbGFzcyA9IC0+XHJcbiAgICAgICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlQmFja1ByZXNzdXJlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5kaXJlY3RpdmUgJ2luZGljYXRvclByaW1hcnknLCAoSm9ic1NlcnZpY2UpIC0+XHJcbiAgcmVwbGFjZTogdHJ1ZVxyXG4gIHNjb3BlOiBcclxuICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiXHJcbiAgICBzdGF0dXM6ICdAJ1xyXG5cclxuICB0ZW1wbGF0ZTogXCI8aSB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKScgLz5cIlxyXG4gIFxyXG4gIGxpbms6IChzY29wZSwgZWxlbWVudCwgYXR0cnMpIC0+XHJcbiAgICBzY29wZS5nZXRMYWJlbENsYXNzID0gLT5cclxuICAgICAgJ2ZhIGZhLWNpcmNsZSBpbmRpY2F0b3IgaW5kaWNhdG9yLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cylcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmRpcmVjdGl2ZSAndGFibGVQcm9wZXJ0eScsIC0+XHJcbiAgcmVwbGFjZTogdHJ1ZVxyXG4gIHNjb3BlOlxyXG4gICAgdmFsdWU6ICc9J1xyXG5cclxuICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmRpcmVjdGl2ZSgnYnNMYWJlbCcsIGZ1bmN0aW9uKEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiB7XG4gICAgdHJhbnNjbHVkZTogdHJ1ZSxcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBnZXRMYWJlbENsYXNzOiBcIiZcIixcbiAgICAgIHN0YXR1czogXCJAXCJcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxzcGFuIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJz48bmctdHJhbnNjbHVkZT48L25nLXRyYW5zY2x1ZGU+PC9zcGFuPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldExhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ2JwTGFiZWwnLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHRyYW5zY2x1ZGU6IHRydWUsXG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgZ2V0QmFja1ByZXNzdXJlTGFiZWxDbGFzczogXCImXCIsXG4gICAgICBzdGF0dXM6IFwiQFwiXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldEJhY2tQcmVzc3VyZUxhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIixcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbWVudCwgYXR0cnMpIHtcbiAgICAgIHJldHVybiBzY29wZS5nZXRCYWNrUHJlc3N1cmVMYWJlbENsYXNzID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUJhY2tQcmVzc3VyZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKTtcbiAgICAgIH07XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCdpbmRpY2F0b3JQcmltYXJ5JywgZnVuY3Rpb24oSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIHtcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBnZXRMYWJlbENsYXNzOiBcIiZcIixcbiAgICAgIHN0YXR1czogJ0AnXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8aSB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKScgLz5cIixcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbWVudCwgYXR0cnMpIHtcbiAgICAgIHJldHVybiBzY29wZS5nZXRMYWJlbENsYXNzID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiAnZmEgZmEtY2lyY2xlIGluZGljYXRvciBpbmRpY2F0b3ItJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKTtcbiAgICAgIH07XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCd0YWJsZVByb3BlcnR5JywgZnVuY3Rpb24oKSB7XG4gIHJldHVybiB7XG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgdmFsdWU6ICc9J1xuICAgIH0sXG4gICAgdGVtcGxhdGU6IFwiPHRkIHRpdGxlPVxcXCJ7e3ZhbHVlIHx8ICdOb25lJ319XFxcIj57e3ZhbHVlIHx8ICdOb25lJ319PC90ZD5cIlxuICB9O1xufSk7XG4iLCIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcclxuXHJcbi5maWx0ZXIgXCJhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRcIiwgKGFuZ3VsYXJNb21lbnRDb25maWcpIC0+XHJcbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyID0gKHZhbHVlLCBmb3JtYXQsIGR1cmF0aW9uRm9ybWF0KSAtPlxyXG4gICAgcmV0dXJuIFwiXCIgIGlmIHR5cGVvZiB2YWx1ZSBpcyBcInVuZGVmaW5lZFwiIG9yIHZhbHVlIGlzIG51bGxcclxuXHJcbiAgICBtb21lbnQuZHVyYXRpb24odmFsdWUsIGZvcm1hdCkuZm9ybWF0KGR1cmF0aW9uRm9ybWF0LCB7IHRyaW06IGZhbHNlIH0pXHJcblxyXG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlci4kc3RhdGVmdWwgPSBhbmd1bGFyTW9tZW50Q29uZmlnLnN0YXRlZnVsRmlsdGVyc1xyXG5cclxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXJcclxuXHJcbi5maWx0ZXIgXCJodW1hbml6ZUR1cmF0aW9uXCIsIC0+XHJcbiAgKHZhbHVlLCBzaG9ydCkgLT5cclxuICAgIHJldHVybiBcIlwiIGlmIHR5cGVvZiB2YWx1ZSBpcyBcInVuZGVmaW5lZFwiIG9yIHZhbHVlIGlzIG51bGxcclxuICAgIG1zID0gdmFsdWUgJSAxMDAwXHJcbiAgICB4ID0gTWF0aC5mbG9vcih2YWx1ZSAvIDEwMDApXHJcbiAgICBzZWNvbmRzID0geCAlIDYwXHJcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gNjApXHJcbiAgICBtaW51dGVzID0geCAlIDYwXHJcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gNjApXHJcbiAgICBob3VycyA9IHggJSAyNFxyXG4gICAgeCA9IE1hdGguZmxvb3IoeCAvIDI0KVxyXG4gICAgZGF5cyA9IHhcclxuICAgIGlmIGRheXMgPT0gMFxyXG4gICAgICBpZiBob3VycyA9PSAwXHJcbiAgICAgICAgaWYgbWludXRlcyA9PSAwXHJcbiAgICAgICAgICBpZiBzZWNvbmRzID09IDBcclxuICAgICAgICAgICAgcmV0dXJuIG1zICsgXCJtc1wiXHJcbiAgICAgICAgICBlbHNlXHJcbiAgICAgICAgICAgIHJldHVybiBzZWNvbmRzICsgXCJzIFwiXHJcbiAgICAgICAgZWxzZVxyXG4gICAgICAgICAgcmV0dXJuIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCJcclxuICAgICAgZWxzZVxyXG4gICAgICAgIGlmIHNob3J0IHRoZW4gcmV0dXJuIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibVwiIGVsc2UgcmV0dXJuIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIlxyXG4gICAgZWxzZVxyXG4gICAgICBpZiBzaG9ydCB0aGVuIHJldHVybiBkYXlzICsgXCJkIFwiICsgaG91cnMgKyBcImhcIiBlbHNlIHJldHVybiBkYXlzICsgXCJkIFwiICsgaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiXHJcblxyXG4uZmlsdGVyIFwiaHVtYW5pemVUZXh0XCIsIC0+XHJcbiAgKHRleHQpIC0+XHJcbiAgICAjIFRPRE86IGV4dGVuZC4uLiBhIGxvdFxyXG4gICAgaWYgdGV4dCB0aGVuIHRleHQucmVwbGFjZSgvJmd0Oy9nLCBcIj5cIikucmVwbGFjZSgvPGJyXFwvPi9nLFwiXCIpIGVsc2UgJydcclxuXHJcbi5maWx0ZXIgXCJodW1hbml6ZUJ5dGVzXCIsIC0+XHJcbiAgKGJ5dGVzKSAtPlxyXG4gICAgdW5pdHMgPSBbXCJCXCIsIFwiS0JcIiwgXCJNQlwiLCBcIkdCXCIsIFwiVEJcIiwgXCJQQlwiLCBcIkVCXCJdXHJcbiAgICBjb252ZXJ0ZXIgPSAodmFsdWUsIHBvd2VyKSAtPlxyXG4gICAgICBiYXNlID0gTWF0aC5wb3coMTAyNCwgcG93ZXIpXHJcbiAgICAgIGlmIHZhbHVlIDwgYmFzZVxyXG4gICAgICAgIHJldHVybiAodmFsdWUgLyBiYXNlKS50b0ZpeGVkKDIpICsgXCIgXCIgKyB1bml0c1twb3dlcl1cclxuICAgICAgZWxzZSBpZiB2YWx1ZSA8IGJhc2UgKiAxMDAwXHJcbiAgICAgICAgcmV0dXJuICh2YWx1ZSAvIGJhc2UpLnRvUHJlY2lzaW9uKDMpICsgXCIgXCIgKyB1bml0c1twb3dlcl1cclxuICAgICAgZWxzZVxyXG4gICAgICAgIHJldHVybiBjb252ZXJ0ZXIodmFsdWUsIHBvd2VyICsgMSlcclxuICAgIHJldHVybiBcIlwiIGlmIHR5cGVvZiBieXRlcyBpcyBcInVuZGVmaW5lZFwiIG9yIGJ5dGVzIGlzIG51bGxcclxuICAgIGlmIGJ5dGVzIDwgMTAwMCB0aGVuIGJ5dGVzICsgXCIgQlwiIGVsc2UgY29udmVydGVyKGJ5dGVzLCAxKVxyXG5cclxuLmZpbHRlciBcInRvTG9jYWxlU3RyaW5nXCIsIC0+XHJcbiAgKHRleHQpIC0+IHRleHQudG9Mb2NhbGVTdHJpbmcoKVxyXG5cclxuLmZpbHRlciBcInRvVXBwZXJDYXNlXCIsIC0+XHJcbiAgKHRleHQpIC0+IHRleHQudG9VcHBlckNhc2UoKVxyXG5cclxuLmZpbHRlciBcInBlcmNlbnRhZ2VcIiwgLT5cclxuICAobnVtYmVyKSAtPiAobnVtYmVyICogMTAwKS50b0ZpeGVkKDApICsgJyUnXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmZpbHRlcihcImFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZFwiLCBmdW5jdGlvbihhbmd1bGFyTW9tZW50Q29uZmlnKSB7XG4gIHZhciBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlciA9IGZ1bmN0aW9uKHZhbHVlLCBmb3JtYXQsIGR1cmF0aW9uRm9ybWF0KSB7XG4gICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gXCJ1bmRlZmluZWRcIiB8fCB2YWx1ZSA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFwiXCI7XG4gICAgfVxuICAgIHJldHVybiBtb21lbnQuZHVyYXRpb24odmFsdWUsIGZvcm1hdCkuZm9ybWF0KGR1cmF0aW9uRm9ybWF0LCB7XG4gICAgICB0cmltOiBmYWxzZVxuICAgIH0pO1xuICB9O1xuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIuJHN0YXRlZnVsID0gYW5ndWxhck1vbWVudENvbmZpZy5zdGF0ZWZ1bEZpbHRlcnM7XG4gIHJldHVybiBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG59KS5maWx0ZXIoXCJodW1hbml6ZUR1cmF0aW9uXCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24odmFsdWUsIHNob3J0KSB7XG4gICAgdmFyIGRheXMsIGhvdXJzLCBtaW51dGVzLCBtcywgc2Vjb25kcywgeDtcbiAgICBpZiAodHlwZW9mIHZhbHVlID09PSBcInVuZGVmaW5lZFwiIHx8IHZhbHVlID09PSBudWxsKSB7XG4gICAgICByZXR1cm4gXCJcIjtcbiAgICB9XG4gICAgbXMgPSB2YWx1ZSAlIDEwMDA7XG4gICAgeCA9IE1hdGguZmxvb3IodmFsdWUgLyAxMDAwKTtcbiAgICBzZWNvbmRzID0geCAlIDYwO1xuICAgIHggPSBNYXRoLmZsb29yKHggLyA2MCk7XG4gICAgbWludXRlcyA9IHggJSA2MDtcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gNjApO1xuICAgIGhvdXJzID0geCAlIDI0O1xuICAgIHggPSBNYXRoLmZsb29yKHggLyAyNCk7XG4gICAgZGF5cyA9IHg7XG4gICAgaWYgKGRheXMgPT09IDApIHtcbiAgICAgIGlmIChob3VycyA9PT0gMCkge1xuICAgICAgICBpZiAobWludXRlcyA9PT0gMCkge1xuICAgICAgICAgIGlmIChzZWNvbmRzID09PSAwKSB7XG4gICAgICAgICAgICByZXR1cm4gbXMgKyBcIm1zXCI7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHJldHVybiBzZWNvbmRzICsgXCJzIFwiO1xuICAgICAgICAgIH1cbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIjtcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgaWYgKHNob3J0KSB7XG4gICAgICAgICAgcmV0dXJuIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibVwiO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiBob3VycyArIFwiaCBcIiArIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCI7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKHNob3J0KSB7XG4gICAgICAgIHJldHVybiBkYXlzICsgXCJkIFwiICsgaG91cnMgKyBcImhcIjtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHJldHVybiBkYXlzICsgXCJkIFwiICsgaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiO1xuICAgICAgfVxuICAgIH1cbiAgfTtcbn0pLmZpbHRlcihcImh1bWFuaXplVGV4dFwiLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIGZ1bmN0aW9uKHRleHQpIHtcbiAgICBpZiAodGV4dCkge1xuICAgICAgcmV0dXJuIHRleHQucmVwbGFjZSgvJmd0Oy9nLCBcIj5cIikucmVwbGFjZSgvPGJyXFwvPi9nLCBcIlwiKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuICcnO1xuICAgIH1cbiAgfTtcbn0pLmZpbHRlcihcImh1bWFuaXplQnl0ZXNcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbihieXRlcykge1xuICAgIHZhciBjb252ZXJ0ZXIsIHVuaXRzO1xuICAgIHVuaXRzID0gW1wiQlwiLCBcIktCXCIsIFwiTUJcIiwgXCJHQlwiLCBcIlRCXCIsIFwiUEJcIiwgXCJFQlwiXTtcbiAgICBjb252ZXJ0ZXIgPSBmdW5jdGlvbih2YWx1ZSwgcG93ZXIpIHtcbiAgICAgIHZhciBiYXNlO1xuICAgICAgYmFzZSA9IE1hdGgucG93KDEwMjQsIHBvd2VyKTtcbiAgICAgIGlmICh2YWx1ZSA8IGJhc2UpIHtcbiAgICAgICAgcmV0dXJuICh2YWx1ZSAvIGJhc2UpLnRvRml4ZWQoMikgKyBcIiBcIiArIHVuaXRzW3Bvd2VyXTtcbiAgICAgIH0gZWxzZSBpZiAodmFsdWUgPCBiYXNlICogMTAwMCkge1xuICAgICAgICByZXR1cm4gKHZhbHVlIC8gYmFzZSkudG9QcmVjaXNpb24oMykgKyBcIiBcIiArIHVuaXRzW3Bvd2VyXTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHJldHVybiBjb252ZXJ0ZXIodmFsdWUsIHBvd2VyICsgMSk7XG4gICAgICB9XG4gICAgfTtcbiAgICBpZiAodHlwZW9mIGJ5dGVzID09PSBcInVuZGVmaW5lZFwiIHx8IGJ5dGVzID09PSBudWxsKSB7XG4gICAgICByZXR1cm4gXCJcIjtcbiAgICB9XG4gICAgaWYgKGJ5dGVzIDwgMTAwMCkge1xuICAgICAgcmV0dXJuIGJ5dGVzICsgXCIgQlwiO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gY29udmVydGVyKGJ5dGVzLCAxKTtcbiAgICB9XG4gIH07XG59KS5maWx0ZXIoXCJ0b0xvY2FsZVN0cmluZ1wiLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIGZ1bmN0aW9uKHRleHQpIHtcbiAgICByZXR1cm4gdGV4dC50b0xvY2FsZVN0cmluZygpO1xuICB9O1xufSkuZmlsdGVyKFwidG9VcHBlckNhc2VcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbih0ZXh0KSB7XG4gICAgcmV0dXJuIHRleHQudG9VcHBlckNhc2UoKTtcbiAgfTtcbn0pLmZpbHRlcihcInBlcmNlbnRhZ2VcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbihudW1iZXIpIHtcbiAgICByZXR1cm4gKG51bWJlciAqIDEwMCkudG9GaXhlZCgwKSArICclJztcbiAgfTtcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4uc2VydmljZSAnTWFpblNlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cclxuICBAbG9hZENvbmZpZyA9IC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJjb25maWdcIlxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuXHJcbiAgQFxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdNYWluU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkQ29uZmlnID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJjb25maWdcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcclxuXHJcbi5jb250cm9sbGVyICdKb2JNYW5hZ2VyQ29uZmlnQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlKSAtPlxyXG4gIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuIChkYXRhKSAtPlxyXG4gICAgaWYgISRzY29wZS5qb2JtYW5hZ2VyP1xyXG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9XHJcbiAgICAkc2NvcGUuam9ibWFuYWdlclsnY29uZmlnJ10gPSBkYXRhXHJcblxyXG4uY29udHJvbGxlciAnSm9iTWFuYWdlckxvZ3NDb250cm9sbGVyJywgKCRzY29wZSwgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlKSAtPlxyXG4gIEpvYk1hbmFnZXJMb2dzU2VydmljZS5sb2FkTG9ncygpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICBpZiAhJHNjb3BlLmpvYm1hbmFnZXI/XHJcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge31cclxuICAgICRzY29wZS5qb2JtYW5hZ2VyWydsb2cnXSA9IGRhdGFcclxuXHJcbiAgJHNjb3BlLnJlbG9hZERhdGEgPSAoKSAtPlxyXG4gICAgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlLmxvYWRMb2dzKCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgJHNjb3BlLmpvYm1hbmFnZXJbJ2xvZyddID0gZGF0YVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYk1hbmFnZXJTdGRvdXRDb250cm9sbGVyJywgKCRzY29wZSwgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UpIC0+XHJcbiAgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UubG9hZFN0ZG91dCgpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICBpZiAhJHNjb3BlLmpvYm1hbmFnZXI/XHJcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge31cclxuICAgICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGFcclxuXHJcbiAgJHNjb3BlLnJlbG9hZERhdGEgPSAoKSAtPlxyXG4gICAgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UubG9hZFN0ZG91dCgpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGFcclxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignSm9iTWFuYWdlckNvbmZpZ0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlKSB7XG4gIHJldHVybiBKb2JNYW5hZ2VyQ29uZmlnU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgaWYgKCRzY29wZS5qb2JtYW5hZ2VyID09IG51bGwpIHtcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge307XG4gICAgfVxuICAgIHJldHVybiAkc2NvcGUuam9ibWFuYWdlclsnY29uZmlnJ10gPSBkYXRhO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYk1hbmFnZXJMb2dzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlKSB7XG4gIEpvYk1hbmFnZXJMb2dzU2VydmljZS5sb2FkTG9ncygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIGlmICgkc2NvcGUuam9ibWFuYWdlciA9PSBudWxsKSB7XG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9O1xuICAgIH1cbiAgICByZXR1cm4gJHNjb3BlLmpvYm1hbmFnZXJbJ2xvZyddID0gZGF0YTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUucmVsb2FkRGF0YSA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JNYW5hZ2VyTG9nc1NlcnZpY2UubG9hZExvZ3MoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUuam9ibWFuYWdlclsnbG9nJ10gPSBkYXRhO1xuICAgIH0pO1xuICB9O1xufSkuY29udHJvbGxlcignSm9iTWFuYWdlclN0ZG91dENvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlKSB7XG4gIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlLmxvYWRTdGRvdXQoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICBpZiAoJHNjb3BlLmpvYm1hbmFnZXIgPT0gbnVsbCkge1xuICAgICAgJHNjb3BlLmpvYm1hbmFnZXIgPSB7fTtcbiAgICB9XG4gICAgcmV0dXJuICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGE7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLnJlbG9hZERhdGEgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UubG9hZFN0ZG91dCgpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGE7XG4gICAgfSk7XG4gIH07XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLnNlcnZpY2UgJ0pvYk1hbmFnZXJDb25maWdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XHJcbiAgY29uZmlnID0ge31cclxuXHJcbiAgQGxvYWRDb25maWcgPSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9ibWFuYWdlci9jb25maWdcIilcclxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cclxuICAgICAgY29uZmlnID0gZGF0YVxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAXHJcblxyXG4uc2VydmljZSAnSm9iTWFuYWdlckxvZ3NTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XHJcbiAgbG9ncyA9IHt9XHJcblxyXG4gIEBsb2FkTG9ncyA9IC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL2xvZ1wiKVxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBsb2dzID0gZGF0YVxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAXHJcblxyXG4uc2VydmljZSAnSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cclxuICBzdGRvdXQgPSB7fVxyXG5cclxuICBAbG9hZFN0ZG91dCA9IC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL3N0ZG91dFwiKVxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBzdGRvdXQgPSBkYXRhXHJcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBcclxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnSm9iTWFuYWdlckNvbmZpZ1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBjb25maWc7XG4gIGNvbmZpZyA9IHt9O1xuICB0aGlzLmxvYWRDb25maWcgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvY29uZmlnXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIGNvbmZpZyA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KS5zZXJ2aWNlKCdKb2JNYW5hZ2VyTG9nc1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBsb2dzO1xuICBsb2dzID0ge307XG4gIHRoaXMubG9hZExvZ3MgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvbG9nXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIGxvZ3MgPSBkYXRhO1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSkuc2VydmljZSgnSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBzdGRvdXQ7XG4gIHN0ZG91dCA9IHt9O1xuICB0aGlzLmxvYWRTdGRvdXQgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvc3Rkb3V0XCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHN0ZG91dCA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLmNvbnRyb2xsZXIgJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cclxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxyXG4gICAgJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJylcclxuXHJcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcclxuXHJcbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5jb250cm9sbGVyICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cclxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxyXG4gICAgJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpXHJcblxyXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxyXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cclxuICAgIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXHJcblxyXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnU2luZ2xlSm9iQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgTWV0cmljc1NlcnZpY2UsICRyb290U2NvcGUsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIC0+XHJcbiAgJHNjb3BlLmpvYmlkID0gJHN0YXRlUGFyYW1zLmpvYmlkXHJcbiAgJHNjb3BlLmpvYiA9IG51bGxcclxuICAkc2NvcGUucGxhbiA9IG51bGxcclxuICAkc2NvcGUudmVydGljZXMgPSBudWxsXHJcbiAgJHNjb3BlLmJhY2tQcmVzc3VyZU9wZXJhdG9yU3RhdHMgPSB7fVxyXG5cclxuICBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbiAoZGF0YSkgLT5cclxuICAgICRzY29wZS5qb2IgPSBkYXRhXHJcbiAgICAkc2NvcGUucGxhbiA9IGRhdGEucGxhblxyXG4gICAgJHNjb3BlLnZlcnRpY2VzID0gZGF0YS52ZXJ0aWNlc1xyXG4gICAgTWV0cmljc1NlcnZpY2Uuc2V0dXBNZXRyaWNzKCRzdGF0ZVBhcmFtcy5qb2JpZCwgZGF0YS52ZXJ0aWNlcylcclxuXHJcbiAgcmVmcmVzaGVyID0gJGludGVydmFsIC0+XHJcbiAgICBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgJHNjb3BlLmpvYiA9IGRhdGFcclxuXHJcbiAgICAgICRzY29wZS4kYnJvYWRjYXN0ICdyZWxvYWQnXHJcblxyXG4gICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXHJcblxyXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cclxuICAgICRzY29wZS5qb2IgPSBudWxsXHJcbiAgICAkc2NvcGUucGxhbiA9IG51bGxcclxuICAgICRzY29wZS52ZXJ0aWNlcyA9IG51bGxcclxuICAgICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzID0gbnVsbFxyXG5cclxuICAgICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaGVyKVxyXG5cclxuICAkc2NvcGUuY2FuY2VsSm9iID0gKGNhbmNlbEV2ZW50KSAtPlxyXG4gICAgYW5ndWxhci5lbGVtZW50KGNhbmNlbEV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKFwiYnRuXCIpLnJlbW92ZUNsYXNzKFwiYnRuLWRlZmF1bHRcIikuaHRtbCgnQ2FuY2VsbGluZy4uLicpXHJcbiAgICBKb2JzU2VydmljZS5jYW5jZWxKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICB7fVxyXG5cclxuICAkc2NvcGUuc3RvcEpvYiA9IChzdG9wRXZlbnQpIC0+XHJcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3RvcEV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKFwiYnRuXCIpLnJlbW92ZUNsYXNzKFwiYnRuLWRlZmF1bHRcIikuaHRtbCgnU3RvcHBpbmcuLi4nKVxyXG4gICAgSm9ic1NlcnZpY2Uuc3RvcEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgIHt9XHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnSm9iUGxhbkNvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgJHdpbmRvdywgSm9ic1NlcnZpY2UpIC0+XHJcbiAgJHNjb3BlLm5vZGVpZCA9IG51bGxcclxuICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2VcclxuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KClcclxuXHJcbiAgJHNjb3BlLmNoYW5nZU5vZGUgPSAobm9kZWlkKSAtPlxyXG4gICAgaWYgbm9kZWlkICE9ICRzY29wZS5ub2RlaWRcclxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG5vZGVpZFxyXG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbFxyXG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXHJcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXHJcbiAgICAgICRzY29wZS5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9IG51bGxcclxuXHJcbiAgICAgICRzY29wZS4kYnJvYWRjYXN0ICdyZWxvYWQnXHJcbiAgICAgICRzY29wZS4kYnJvYWRjYXN0ICdub2RlOmNoYW5nZScsICRzY29wZS5ub2RlaWRcclxuXHJcbiAgICBlbHNlXHJcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsXHJcbiAgICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxyXG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbFxyXG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXHJcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXHJcbiAgICAgICRzY29wZS5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9IG51bGxcclxuXHJcbiAgJHNjb3BlLmRlYWN0aXZhdGVOb2RlID0gLT5cclxuICAgICRzY29wZS5ub2RlaWQgPSBudWxsXHJcbiAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2VcclxuICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXHJcbiAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXHJcbiAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxyXG4gICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbFxyXG5cclxuICAkc2NvcGUudG9nZ2xlRm9sZCA9IC0+XHJcbiAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gISRzY29wZS5ub2RlVW5mb2xkZWRcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5jb250cm9sbGVyICdKb2JQbGFuU3VidGFza3NDb250cm9sbGVyJywgKCRzY29wZSwgSm9ic1NlcnZpY2UpIC0+XHJcbiAgZ2V0U3VidGFza3MgPSAtPlxyXG4gICAgSm9ic1NlcnZpY2UuZ2V0U3VidGFza3MoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gZGF0YVxyXG5cclxuICBpZiAkc2NvcGUubm9kZWlkIGFuZCAoISRzY29wZS52ZXJ0ZXggb3IgISRzY29wZS52ZXJ0ZXguc3QpXHJcbiAgICBnZXRTdWJ0YXNrcygpXHJcblxyXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cclxuICAgIGdldFN1YnRhc2tzKCkgaWYgJHNjb3BlLm5vZGVpZFxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5UYXNrTWFuYWdlcnNDb250cm9sbGVyJywgKCRzY29wZSwgSm9ic1NlcnZpY2UpIC0+XHJcbiAgZ2V0VGFza01hbmFnZXJzID0gLT5cclxuICAgIEpvYnNTZXJ2aWNlLmdldFRhc2tNYW5hZ2Vycygkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAkc2NvcGUudGFza21hbmFnZXJzID0gZGF0YVxyXG5cclxuICBpZiAkc2NvcGUubm9kZWlkIGFuZCAoISRzY29wZS52ZXJ0ZXggb3IgISRzY29wZS52ZXJ0ZXguc3QpXHJcbiAgICBnZXRUYXNrTWFuYWdlcnMoKVxyXG5cclxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XHJcbiAgICBnZXRUYXNrTWFuYWdlcnMoKSBpZiAkc2NvcGUubm9kZWlkXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cclxuICBnZXRBY2N1bXVsYXRvcnMgPSAtPlxyXG4gICAgSm9ic1NlcnZpY2UuZ2V0QWNjdW11bGF0b3JzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBkYXRhLm1haW5cclxuICAgICAgJHNjb3BlLnN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzXHJcblxyXG4gIGlmICRzY29wZS5ub2RlaWQgYW5kICghJHNjb3BlLnZlcnRleCBvciAhJHNjb3BlLnZlcnRleC5hY2N1bXVsYXRvcnMpXHJcbiAgICBnZXRBY2N1bXVsYXRvcnMoKVxyXG5cclxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XHJcbiAgICBnZXRBY2N1bXVsYXRvcnMoKSBpZiAkc2NvcGUubm9kZWlkXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cclxuICAjIFVwZGF0ZWQgYnkgdGhlIGRldGFpbHMgaGFuZGxlciBmb3IgdGhlIHN1YiBjaGVja3BvaW50cyBuYXYgYmFyLlxyXG4gICRzY29wZS5jaGVja3BvaW50RGV0YWlscyA9IHt9XHJcbiAgJHNjb3BlLmNoZWNrcG9pbnREZXRhaWxzLmlkID0gLTFcclxuXHJcbiAgIyBSZXF1ZXN0IHRoZSBjb25maWcgb25jZSAoaXQncyBzdGF0aWMpXHJcbiAgSm9ic1NlcnZpY2UuZ2V0Q2hlY2twb2ludENvbmZpZygpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAkc2NvcGUuY2hlY2twb2ludENvbmZpZyA9IGRhdGFcclxuXHJcbiAgIyBHZW5lcmFsIHN0YXRzIGxpa2UgY291bnRzLCBoaXN0b3J5LCBldGMuXHJcbiAgZ2V0R2VuZXJhbENoZWNrcG9pbnRTdGF0cyA9IC0+XHJcbiAgICBKb2JzU2VydmljZS5nZXRDaGVja3BvaW50U3RhdHMoKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICBpZiAoZGF0YSAhPSBudWxsKVxyXG4gICAgICAgICRzY29wZS5jaGVja3BvaW50U3RhdHMgPSBkYXRhXHJcblxyXG4gICMgVHJpZ2dlciByZXF1ZXN0XHJcbiAgZ2V0R2VuZXJhbENoZWNrcG9pbnRTdGF0cygpXHJcblxyXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cclxuICAgICMgUmV0cmlnZ2VyIHJlcXVlc3RcclxuICAgIGdldEdlbmVyYWxDaGVja3BvaW50U3RhdHMoKVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5DaGVja3BvaW50RGV0YWlsc0NvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XHJcbiAgJHNjb3BlLnN1YnRhc2tEZXRhaWxzID0ge31cclxuICAkc2NvcGUuY2hlY2twb2ludERldGFpbHMuaWQgPSAkc3RhdGVQYXJhbXMuY2hlY2twb2ludElkXHJcblxyXG4gICMgRGV0YWlsZWQgc3RhdHMgZm9yIGEgc2luZ2xlIGNoZWNrcG9pbnRcclxuICBnZXRDaGVja3BvaW50RGV0YWlscyA9IChjaGVja3BvaW50SWQpIC0+XHJcbiAgICBKb2JzU2VydmljZS5nZXRDaGVja3BvaW50RGV0YWlscyhjaGVja3BvaW50SWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgIGlmIChkYXRhICE9IG51bGwpXHJcbiAgICAgICAgJHNjb3BlLmNoZWNrcG9pbnQgPSBkYXRhXHJcbiAgICAgIGVsc2VcclxuICAgICAgICAkc2NvcGUudW5rbm93bl9jaGVja3BvaW50ID0gdHJ1ZVxyXG5cclxuICBnZXRDaGVja3BvaW50U3VidGFza0RldGFpbHMgPSAoY2hlY2twb2ludElkLCB2ZXJ0ZXhJZCkgLT5cclxuICAgIEpvYnNTZXJ2aWNlLmdldENoZWNrcG9pbnRTdWJ0YXNrRGV0YWlscyhjaGVja3BvaW50SWQsIHZlcnRleElkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICBpZiAoZGF0YSAhPSBudWxsKVxyXG4gICAgICAgICRzY29wZS5zdWJ0YXNrRGV0YWlsc1t2ZXJ0ZXhJZF0gPSBkYXRhXHJcblxyXG4gIGdldENoZWNrcG9pbnREZXRhaWxzKCRzdGF0ZVBhcmFtcy5jaGVja3BvaW50SWQpXHJcblxyXG4gIGlmICgkc2NvcGUubm9kZWlkKVxyXG4gICAgZ2V0Q2hlY2twb2ludFN1YnRhc2tEZXRhaWxzKCRzdGF0ZVBhcmFtcy5jaGVja3BvaW50SWQsICRzY29wZS5ub2RlaWQpXHJcblxyXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cclxuICAgIGdldENoZWNrcG9pbnREZXRhaWxzKCRzdGF0ZVBhcmFtcy5jaGVja3BvaW50SWQpXHJcblxyXG4gICAgaWYgKCRzY29wZS5ub2RlaWQpXHJcbiAgICAgIGdldENoZWNrcG9pbnRTdWJ0YXNrRGV0YWlscygkc3RhdGVQYXJhbXMuY2hlY2twb2ludElkLCAkc2NvcGUubm9kZWlkKVxyXG5cclxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XHJcbiAgICAkc2NvcGUuY2hlY2twb2ludERldGFpbHMuaWQgPSAtMVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyJywgKCRzY29wZSwgSm9ic1NlcnZpY2UpIC0+XHJcbiAgZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmUgPSAtPlxyXG4gICAgJHNjb3BlLm5vdyA9IERhdGUubm93KClcclxuXHJcbiAgICBpZiAkc2NvcGUubm9kZWlkXHJcbiAgICAgIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQmFja1ByZXNzdXJlKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICAgJHNjb3BlLmJhY2tQcmVzc3VyZU9wZXJhdG9yU3RhdHNbJHNjb3BlLm5vZGVpZF0gPSBkYXRhXHJcblxyXG4gIGdldE9wZXJhdG9yQmFja1ByZXNzdXJlKClcclxuXHJcbiAgJHNjb3BlLiRvbiAncmVsb2FkJywgKGV2ZW50KSAtPlxyXG4gICAgZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmUoKVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cclxuICBnZXRWZXJ0ZXggPSAtPlxyXG4gICAgSm9ic1NlcnZpY2UuZ2V0VmVydGV4KCRzdGF0ZVBhcmFtcy52ZXJ0ZXhJZCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgJHNjb3BlLnZlcnRleCA9IGRhdGFcclxuXHJcbiAgZ2V0VmVydGV4KClcclxuXHJcbiAgJHNjb3BlLiRvbiAncmVsb2FkJywgKGV2ZW50KSAtPlxyXG4gICAgZ2V0VmVydGV4KClcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5jb250cm9sbGVyICdKb2JFeGNlcHRpb25zQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cclxuICBKb2JzU2VydmljZS5sb2FkRXhjZXB0aW9ucygpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAkc2NvcGUuZXhjZXB0aW9ucyA9IGRhdGFcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5jb250cm9sbGVyICdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYnNTZXJ2aWNlKSAtPlxyXG4gICRzY29wZS5jaGFuZ2VOb2RlID0gKG5vZGVpZCkgLT5cclxuICAgIGlmIG5vZGVpZCAhPSAkc2NvcGUubm9kZWlkXHJcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWRcclxuXHJcbiAgICAgIEpvYnNTZXJ2aWNlLmdldE5vZGUobm9kZWlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAgICRzY29wZS5ub2RlID0gZGF0YVxyXG5cclxuICAgIGVsc2VcclxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG51bGxcclxuICAgICAgJHNjb3BlLm5vZGUgPSBudWxsXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnSm9iUGxhbk1ldHJpY3NDb250cm9sbGVyJywgKCRzY29wZSwgSm9ic1NlcnZpY2UsIE1ldHJpY3NTZXJ2aWNlKSAtPlxyXG4gICRzY29wZS5kcmFnZ2luZyA9IGZhbHNlXHJcbiAgJHNjb3BlLndpbmRvdyA9IE1ldHJpY3NTZXJ2aWNlLmdldFdpbmRvdygpXHJcbiAgJHNjb3BlLmF2YWlsYWJsZU1ldHJpY3MgPSBudWxsXHJcblxyXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cclxuICAgIE1ldHJpY3NTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigpXHJcblxyXG4gIGxvYWRNZXRyaWNzID0gLT5cclxuICAgIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAkc2NvcGUudmVydGV4ID0gZGF0YVxyXG5cclxuICAgIE1ldHJpY3NTZXJ2aWNlLmdldEF2YWlsYWJsZU1ldHJpY3MoJHNjb3BlLmpvYmlkLCAkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAkc2NvcGUuYXZhaWxhYmxlTWV0cmljcyA9IGRhdGFcclxuICAgICAgJHNjb3BlLm1ldHJpY3MgPSBNZXRyaWNzU2VydmljZS5nZXRNZXRyaWNzU2V0dXAoJHNjb3BlLmpvYmlkLCAkc2NvcGUubm9kZWlkKS5uYW1lc1xyXG5cclxuICAgICAgTWV0cmljc1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQsIChkYXRhKSAtPlxyXG4gICAgICAgICRzY29wZS4kYnJvYWRjYXN0IFwibWV0cmljczpkYXRhOnVwZGF0ZVwiLCBkYXRhLnRpbWVzdGFtcCwgZGF0YS52YWx1ZXNcclxuICAgICAgKVxyXG5cclxuICAkc2NvcGUuZHJvcHBlZCA9IChldmVudCwgaW5kZXgsIGl0ZW0sIGV4dGVybmFsLCB0eXBlKSAtPlxyXG5cclxuICAgIE1ldHJpY3NTZXJ2aWNlLm9yZGVyTWV0cmljcygkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQsIGl0ZW0sIGluZGV4KVxyXG4gICAgJHNjb3BlLiRicm9hZGNhc3QgXCJtZXRyaWNzOnJlZnJlc2hcIiwgaXRlbVxyXG4gICAgbG9hZE1ldHJpY3MoKVxyXG4gICAgZmFsc2VcclxuXHJcbiAgJHNjb3BlLmRyYWdTdGFydCA9IC0+XHJcbiAgICAkc2NvcGUuZHJhZ2dpbmcgPSB0cnVlXHJcblxyXG4gICRzY29wZS5kcmFnRW5kID0gLT5cclxuICAgICRzY29wZS5kcmFnZ2luZyA9IGZhbHNlXHJcblxyXG4gICRzY29wZS5hZGRNZXRyaWMgPSAobWV0cmljKSAtPlxyXG4gICAgTWV0cmljc1NlcnZpY2UuYWRkTWV0cmljKCRzY29wZS5qb2JpZCwgJHNjb3BlLm5vZGVpZCwgbWV0cmljLmlkKVxyXG4gICAgbG9hZE1ldHJpY3MoKVxyXG5cclxuICAkc2NvcGUucmVtb3ZlTWV0cmljID0gKG1ldHJpYykgLT5cclxuICAgIE1ldHJpY3NTZXJ2aWNlLnJlbW92ZU1ldHJpYygkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQsIG1ldHJpYylcclxuICAgIGxvYWRNZXRyaWNzKClcclxuXHJcbiAgJHNjb3BlLnNldE1ldHJpY1NpemUgPSAobWV0cmljLCBzaXplKSAtPlxyXG4gICAgTWV0cmljc1NlcnZpY2Uuc2V0TWV0cmljU2l6ZSgkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQsIG1ldHJpYywgc2l6ZSlcclxuICAgIGxvYWRNZXRyaWNzKClcclxuXHJcbiAgJHNjb3BlLmdldFZhbHVlcyA9IChtZXRyaWMpIC0+XHJcbiAgICBNZXRyaWNzU2VydmljZS5nZXRWYWx1ZXMoJHNjb3BlLmpvYmlkLCAkc2NvcGUubm9kZWlkLCBtZXRyaWMpXHJcblxyXG4gICRzY29wZS4kb24gJ25vZGU6Y2hhbmdlJywgKGV2ZW50LCBub2RlaWQpIC0+XHJcbiAgICBsb2FkTWV0cmljcygpIGlmICEkc2NvcGUuZHJhZ2dpbmdcclxuXHJcbiAgbG9hZE1ldHJpY3MoKSBpZiAkc2NvcGUubm9kZWlkXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbn0pLmNvbnRyb2xsZXIoJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZUpvYkNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgTWV0cmljc1NlcnZpY2UsICRyb290U2NvcGUsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIHtcbiAgdmFyIHJlZnJlc2hlcjtcbiAgJHNjb3BlLmpvYmlkID0gJHN0YXRlUGFyYW1zLmpvYmlkO1xuICAkc2NvcGUuam9iID0gbnVsbDtcbiAgJHNjb3BlLnBsYW4gPSBudWxsO1xuICAkc2NvcGUudmVydGljZXMgPSBudWxsO1xuICAkc2NvcGUuYmFja1ByZXNzdXJlT3BlcmF0b3JTdGF0cyA9IHt9O1xuICBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgJHNjb3BlLmpvYiA9IGRhdGE7XG4gICAgJHNjb3BlLnBsYW4gPSBkYXRhLnBsYW47XG4gICAgJHNjb3BlLnZlcnRpY2VzID0gZGF0YS52ZXJ0aWNlcztcbiAgICByZXR1cm4gTWV0cmljc1NlcnZpY2Uuc2V0dXBNZXRyaWNzKCRzdGF0ZVBhcmFtcy5qb2JpZCwgZGF0YS52ZXJ0aWNlcyk7XG4gIH0pO1xuICByZWZyZXNoZXIgPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICRzY29wZS5qb2IgPSBkYXRhO1xuICAgICAgcmV0dXJuICRzY29wZS4kYnJvYWRjYXN0KCdyZWxvYWQnKTtcbiAgICB9KTtcbiAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUuam9iID0gbnVsbDtcbiAgICAkc2NvcGUucGxhbiA9IG51bGw7XG4gICAgJHNjb3BlLnZlcnRpY2VzID0gbnVsbDtcbiAgICAkc2NvcGUuYmFja1ByZXNzdXJlT3BlcmF0b3JTdGF0cyA9IG51bGw7XG4gICAgcmV0dXJuICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaGVyKTtcbiAgfSk7XG4gICRzY29wZS5jYW5jZWxKb2IgPSBmdW5jdGlvbihjYW5jZWxFdmVudCkge1xuICAgIGFuZ3VsYXIuZWxlbWVudChjYW5jZWxFdmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImJ0blwiKS5yZW1vdmVDbGFzcyhcImJ0bi1kZWZhdWx0XCIpLmh0bWwoJ0NhbmNlbGxpbmcuLi4nKTtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UuY2FuY2VsSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4ge307XG4gICAgfSk7XG4gIH07XG4gIHJldHVybiAkc2NvcGUuc3RvcEpvYiA9IGZ1bmN0aW9uKHN0b3BFdmVudCkge1xuICAgIGFuZ3VsYXIuZWxlbWVudChzdG9wRXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJidG5cIikucmVtb3ZlQ2xhc3MoXCJidG4tZGVmYXVsdFwiKS5odG1sKCdTdG9wcGluZy4uLicpO1xuICAgIHJldHVybiBKb2JzU2VydmljZS5zdG9wSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4ge307XG4gICAgfSk7XG4gIH07XG59KS5jb250cm9sbGVyKCdKb2JQbGFuQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsICR3aW5kb3csIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2U7XG4gICRzY29wZS5zdGF0ZUxpc3QgPSBKb2JzU2VydmljZS5zdGF0ZUxpc3QoKTtcbiAgJHNjb3BlLmNoYW5nZU5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICBpZiAobm9kZWlkICE9PSAkc2NvcGUubm9kZWlkKSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkO1xuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGw7XG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGw7XG4gICAgICAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBudWxsO1xuICAgICAgJHNjb3BlLiRicm9hZGNhc3QoJ3JlbG9hZCcpO1xuICAgICAgcmV0dXJuICRzY29wZS4kYnJvYWRjYXN0KCdub2RlOmNoYW5nZScsICRzY29wZS5ub2RlaWQpO1xuICAgIH0gZWxzZSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZTtcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsO1xuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbDtcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICAgICAgcmV0dXJuICRzY29wZS5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9IG51bGw7XG4gICAgfVxuICB9O1xuICAkc2NvcGUuZGVhY3RpdmF0ZU5vZGUgPSBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2U7XG4gICAgJHNjb3BlLnZlcnRleCA9IG51bGw7XG4gICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbDtcbiAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbDtcbiAgICByZXR1cm4gJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbDtcbiAgfTtcbiAgcmV0dXJuICRzY29wZS50b2dnbGVGb2xkID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5ub2RlVW5mb2xkZWQgPSAhJHNjb3BlLm5vZGVVbmZvbGRlZDtcbiAgfTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5TdWJ0YXNrc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYnNTZXJ2aWNlKSB7XG4gIHZhciBnZXRTdWJ0YXNrcztcbiAgZ2V0U3VidGFza3MgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0U3VidGFza3MoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLnN1YnRhc2tzID0gZGF0YTtcbiAgICB9KTtcbiAgfTtcbiAgaWYgKCRzY29wZS5ub2RlaWQgJiYgKCEkc2NvcGUudmVydGV4IHx8ICEkc2NvcGUudmVydGV4LnN0KSkge1xuICAgIGdldFN1YnRhc2tzKCk7XG4gIH1cbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICAgIHJldHVybiBnZXRTdWJ0YXNrcygpO1xuICAgIH1cbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuVGFza01hbmFnZXJzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgdmFyIGdldFRhc2tNYW5hZ2VycztcbiAgZ2V0VGFza01hbmFnZXJzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldFRhc2tNYW5hZ2Vycygkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUudGFza21hbmFnZXJzID0gZGF0YTtcbiAgICB9KTtcbiAgfTtcbiAgaWYgKCRzY29wZS5ub2RlaWQgJiYgKCEkc2NvcGUudmVydGV4IHx8ICEkc2NvcGUudmVydGV4LnN0KSkge1xuICAgIGdldFRhc2tNYW5hZ2VycygpO1xuICB9XG4gIHJldHVybiAkc2NvcGUuJG9uKCdyZWxvYWQnLCBmdW5jdGlvbihldmVudCkge1xuICAgIGlmICgkc2NvcGUubm9kZWlkKSB7XG4gICAgICByZXR1cm4gZ2V0VGFza01hbmFnZXJzKCk7XG4gICAgfVxuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JzU2VydmljZSkge1xuICB2YXIgZ2V0QWNjdW11bGF0b3JzO1xuICBnZXRBY2N1bXVsYXRvcnMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0QWNjdW11bGF0b3JzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IGRhdGEubWFpbjtcbiAgICAgIHJldHVybiAkc2NvcGUuc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3M7XG4gICAgfSk7XG4gIH07XG4gIGlmICgkc2NvcGUubm9kZWlkICYmICghJHNjb3BlLnZlcnRleCB8fCAhJHNjb3BlLnZlcnRleC5hY2N1bXVsYXRvcnMpKSB7XG4gICAgZ2V0QWNjdW11bGF0b3JzKCk7XG4gIH1cbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICAgIHJldHVybiBnZXRBY2N1bXVsYXRvcnMoKTtcbiAgICB9XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIHZhciBnZXRHZW5lcmFsQ2hlY2twb2ludFN0YXRzO1xuICAkc2NvcGUuY2hlY2twb2ludERldGFpbHMgPSB7fTtcbiAgJHNjb3BlLmNoZWNrcG9pbnREZXRhaWxzLmlkID0gLTE7XG4gIEpvYnNTZXJ2aWNlLmdldENoZWNrcG9pbnRDb25maWcoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLmNoZWNrcG9pbnRDb25maWcgPSBkYXRhO1xuICB9KTtcbiAgZ2V0R2VuZXJhbENoZWNrcG9pbnRTdGF0cyA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRDaGVja3BvaW50U3RhdHMoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIGlmIChkYXRhICE9PSBudWxsKSB7XG4gICAgICAgIHJldHVybiAkc2NvcGUuY2hlY2twb2ludFN0YXRzID0gZGF0YTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfTtcbiAgZ2V0R2VuZXJhbENoZWNrcG9pbnRTdGF0cygpO1xuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICByZXR1cm4gZ2V0R2VuZXJhbENoZWNrcG9pbnRTdGF0cygpO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5DaGVja3BvaW50RGV0YWlsc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICB2YXIgZ2V0Q2hlY2twb2ludERldGFpbHMsIGdldENoZWNrcG9pbnRTdWJ0YXNrRGV0YWlscztcbiAgJHNjb3BlLnN1YnRhc2tEZXRhaWxzID0ge307XG4gICRzY29wZS5jaGVja3BvaW50RGV0YWlscy5pZCA9ICRzdGF0ZVBhcmFtcy5jaGVja3BvaW50SWQ7XG4gIGdldENoZWNrcG9pbnREZXRhaWxzID0gZnVuY3Rpb24oY2hlY2twb2ludElkKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldENoZWNrcG9pbnREZXRhaWxzKGNoZWNrcG9pbnRJZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICBpZiAoZGF0YSAhPT0gbnVsbCkge1xuICAgICAgICByZXR1cm4gJHNjb3BlLmNoZWNrcG9pbnQgPSBkYXRhO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS51bmtub3duX2NoZWNrcG9pbnQgPSB0cnVlO1xuICAgICAgfVxuICAgIH0pO1xuICB9O1xuICBnZXRDaGVja3BvaW50U3VidGFza0RldGFpbHMgPSBmdW5jdGlvbihjaGVja3BvaW50SWQsIHZlcnRleElkKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldENoZWNrcG9pbnRTdWJ0YXNrRGV0YWlscyhjaGVja3BvaW50SWQsIHZlcnRleElkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIGlmIChkYXRhICE9PSBudWxsKSB7XG4gICAgICAgIHJldHVybiAkc2NvcGUuc3VidGFza0RldGFpbHNbdmVydGV4SWRdID0gZGF0YTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfTtcbiAgZ2V0Q2hlY2twb2ludERldGFpbHMoJHN0YXRlUGFyYW1zLmNoZWNrcG9pbnRJZCk7XG4gIGlmICgkc2NvcGUubm9kZWlkKSB7XG4gICAgZ2V0Q2hlY2twb2ludFN1YnRhc2tEZXRhaWxzKCRzdGF0ZVBhcmFtcy5jaGVja3BvaW50SWQsICRzY29wZS5ub2RlaWQpO1xuICB9XG4gICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgZ2V0Q2hlY2twb2ludERldGFpbHMoJHN0YXRlUGFyYW1zLmNoZWNrcG9pbnRJZCk7XG4gICAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICAgIHJldHVybiBnZXRDaGVja3BvaW50U3VidGFza0RldGFpbHMoJHN0YXRlUGFyYW1zLmNoZWNrcG9pbnRJZCwgJHNjb3BlLm5vZGVpZCk7XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5jaGVja3BvaW50RGV0YWlscy5pZCA9IC0xO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JzU2VydmljZSkge1xuICB2YXIgZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmU7XG4gIGdldE9wZXJhdG9yQmFja1ByZXNzdXJlID0gZnVuY3Rpb24oKSB7XG4gICAgJHNjb3BlLm5vdyA9IERhdGUubm93KCk7XG4gICAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRPcGVyYXRvckJhY2tQcmVzc3VyZSgkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzWyRzY29wZS5ub2RlaWRdID0gZGF0YTtcbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbiAgZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmUoKTtcbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgcmV0dXJuIGdldE9wZXJhdG9yQmFja1ByZXNzdXJlKCk7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgdmFyIGdldFZlcnRleDtcbiAgZ2V0VmVydGV4ID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS52ZXJ0ZXggPSBkYXRhO1xuICAgIH0pO1xuICB9O1xuICBnZXRWZXJ0ZXgoKTtcbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgcmV0dXJuIGdldFZlcnRleCgpO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIEpvYnNTZXJ2aWNlLmxvYWRFeGNlcHRpb25zKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5leGNlcHRpb25zID0gZGF0YTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuICRzY29wZS5jaGFuZ2VOb2RlID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgaWYgKG5vZGVpZCAhPT0gJHNjb3BlLm5vZGVpZCkge1xuICAgICAgJHNjb3BlLm5vZGVpZCA9IG5vZGVpZDtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5nZXROb2RlKG5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkc2NvcGUubm9kZSA9IGRhdGE7XG4gICAgICB9KTtcbiAgICB9IGVsc2Uge1xuICAgICAgJHNjb3BlLm5vZGVpZCA9IG51bGw7XG4gICAgICByZXR1cm4gJHNjb3BlLm5vZGUgPSBudWxsO1xuICAgIH1cbiAgfTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5NZXRyaWNzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UsIE1ldHJpY3NTZXJ2aWNlKSB7XG4gIHZhciBsb2FkTWV0cmljcztcbiAgJHNjb3BlLmRyYWdnaW5nID0gZmFsc2U7XG4gICRzY29wZS53aW5kb3cgPSBNZXRyaWNzU2VydmljZS5nZXRXaW5kb3coKTtcbiAgJHNjb3BlLmF2YWlsYWJsZU1ldHJpY3MgPSBudWxsO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBNZXRyaWNzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoKTtcbiAgfSk7XG4gIGxvYWRNZXRyaWNzID0gZnVuY3Rpb24oKSB7XG4gICAgSm9ic1NlcnZpY2UuZ2V0VmVydGV4KCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS52ZXJ0ZXggPSBkYXRhO1xuICAgIH0pO1xuICAgIHJldHVybiBNZXRyaWNzU2VydmljZS5nZXRBdmFpbGFibGVNZXRyaWNzKCRzY29wZS5qb2JpZCwgJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAkc2NvcGUuYXZhaWxhYmxlTWV0cmljcyA9IGRhdGE7XG4gICAgICAkc2NvcGUubWV0cmljcyA9IE1ldHJpY3NTZXJ2aWNlLmdldE1ldHJpY3NTZXR1cCgkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQpLm5hbWVzO1xuICAgICAgcmV0dXJuIE1ldHJpY3NTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYmlkLCAkc2NvcGUubm9kZWlkLCBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkc2NvcGUuJGJyb2FkY2FzdChcIm1ldHJpY3M6ZGF0YTp1cGRhdGVcIiwgZGF0YS50aW1lc3RhbXAsIGRhdGEudmFsdWVzKTtcbiAgICAgIH0pO1xuICAgIH0pO1xuICB9O1xuICAkc2NvcGUuZHJvcHBlZCA9IGZ1bmN0aW9uKGV2ZW50LCBpbmRleCwgaXRlbSwgZXh0ZXJuYWwsIHR5cGUpIHtcbiAgICBNZXRyaWNzU2VydmljZS5vcmRlck1ldHJpY3MoJHNjb3BlLmpvYmlkLCAkc2NvcGUubm9kZWlkLCBpdGVtLCBpbmRleCk7XG4gICAgJHNjb3BlLiRicm9hZGNhc3QoXCJtZXRyaWNzOnJlZnJlc2hcIiwgaXRlbSk7XG4gICAgbG9hZE1ldHJpY3MoKTtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH07XG4gICRzY29wZS5kcmFnU3RhcnQgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLmRyYWdnaW5nID0gdHJ1ZTtcbiAgfTtcbiAgJHNjb3BlLmRyYWdFbmQgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLmRyYWdnaW5nID0gZmFsc2U7XG4gIH07XG4gICRzY29wZS5hZGRNZXRyaWMgPSBmdW5jdGlvbihtZXRyaWMpIHtcbiAgICBNZXRyaWNzU2VydmljZS5hZGRNZXRyaWMoJHNjb3BlLmpvYmlkLCAkc2NvcGUubm9kZWlkLCBtZXRyaWMuaWQpO1xuICAgIHJldHVybiBsb2FkTWV0cmljcygpO1xuICB9O1xuICAkc2NvcGUucmVtb3ZlTWV0cmljID0gZnVuY3Rpb24obWV0cmljKSB7XG4gICAgTWV0cmljc1NlcnZpY2UucmVtb3ZlTWV0cmljKCRzY29wZS5qb2JpZCwgJHNjb3BlLm5vZGVpZCwgbWV0cmljKTtcbiAgICByZXR1cm4gbG9hZE1ldHJpY3MoKTtcbiAgfTtcbiAgJHNjb3BlLnNldE1ldHJpY1NpemUgPSBmdW5jdGlvbihtZXRyaWMsIHNpemUpIHtcbiAgICBNZXRyaWNzU2VydmljZS5zZXRNZXRyaWNTaXplKCRzY29wZS5qb2JpZCwgJHNjb3BlLm5vZGVpZCwgbWV0cmljLCBzaXplKTtcbiAgICByZXR1cm4gbG9hZE1ldHJpY3MoKTtcbiAgfTtcbiAgJHNjb3BlLmdldFZhbHVlcyA9IGZ1bmN0aW9uKG1ldHJpYykge1xuICAgIHJldHVybiBNZXRyaWNzU2VydmljZS5nZXRWYWx1ZXMoJHNjb3BlLmpvYmlkLCAkc2NvcGUubm9kZWlkLCBtZXRyaWMpO1xuICB9O1xuICAkc2NvcGUuJG9uKCdub2RlOmNoYW5nZScsIGZ1bmN0aW9uKGV2ZW50LCBub2RlaWQpIHtcbiAgICBpZiAoISRzY29wZS5kcmFnZ2luZykge1xuICAgICAgcmV0dXJuIGxvYWRNZXRyaWNzKCk7XG4gICAgfVxuICB9KTtcbiAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICByZXR1cm4gbG9hZE1ldHJpY3MoKTtcbiAgfVxufSk7XG4iLCIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmRpcmVjdGl2ZSAndmVydGV4JywgKCRzdGF0ZSkgLT5cclxuICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZSBzZWNvbmRhcnknIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiXHJcblxyXG4gIHNjb3BlOlxyXG4gICAgZGF0YTogXCI9XCJcclxuXHJcbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cclxuICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdXHJcblxyXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxyXG4gICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpXHJcblxyXG4gICAgYW5hbHl6ZVRpbWUgPSAoZGF0YSkgLT5cclxuICAgICAgZDMuc2VsZWN0KHN2Z0VsKS5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpXHJcblxyXG4gICAgICB0ZXN0RGF0YSA9IFtdXHJcblxyXG4gICAgICBhbmd1bGFyLmZvckVhY2ggZGF0YS5zdWJ0YXNrcywgKHN1YnRhc2ssIGkpIC0+XHJcbiAgICAgICAgdGltZXMgPSBbXHJcbiAgICAgICAgICB7XHJcbiAgICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiXHJcbiAgICAgICAgICAgIGNvbG9yOiBcIiM2NjZcIlxyXG4gICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcclxuICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiU0NIRURVTEVEXCJdXHJcbiAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl1cclxuICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXHJcbiAgICAgICAgICB9XHJcbiAgICAgICAgICB7XHJcbiAgICAgICAgICAgIGxhYmVsOiBcIkRlcGxveWluZ1wiXHJcbiAgICAgICAgICAgIGNvbG9yOiBcIiNhYWFcIlxyXG4gICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcclxuICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdXHJcbiAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJSVU5OSU5HXCJdXHJcbiAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xyXG4gICAgICAgICAgfVxyXG4gICAgICAgIF1cclxuXHJcbiAgICAgICAgaWYgc3VidGFzay50aW1lc3RhbXBzW1wiRklOSVNIRURcIl0gPiAwXHJcbiAgICAgICAgICB0aW1lcy5wdXNoIHtcclxuICAgICAgICAgICAgbGFiZWw6IFwiUnVubmluZ1wiXHJcbiAgICAgICAgICAgIGNvbG9yOiBcIiNkZGRcIlxyXG4gICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcclxuICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXVxyXG4gICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiRklOSVNIRURcIl1cclxuICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXHJcbiAgICAgICAgICB9XHJcblxyXG4gICAgICAgIHRlc3REYXRhLnB1c2gge1xyXG4gICAgICAgICAgbGFiZWw6IFwiKCN7c3VidGFzay5zdWJ0YXNrfSkgI3tzdWJ0YXNrLmhvc3R9XCJcclxuICAgICAgICAgIHRpbWVzOiB0aW1lc1xyXG4gICAgICAgIH1cclxuXHJcbiAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpXHJcbiAgICAgIC50aWNrRm9ybWF0KHtcclxuICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIilcclxuICAgICAgICAjIHRpY2tJbnRlcnZhbDogMVxyXG4gICAgICAgIHRpY2tTaXplOiAxXHJcbiAgICAgIH0pXHJcbiAgICAgIC5wcmVmaXgoXCJzaW5nbGVcIilcclxuICAgICAgLmxhYmVsRm9ybWF0KChsYWJlbCkgLT5cclxuICAgICAgICBsYWJlbFxyXG4gICAgICApXHJcbiAgICAgIC5tYXJnaW4oeyBsZWZ0OiAxMDAsIHJpZ2h0OiAwLCB0b3A6IDAsIGJvdHRvbTogMCB9KVxyXG4gICAgICAuaXRlbUhlaWdodCgzMClcclxuICAgICAgLnJlbGF0aXZlVGltZSgpXHJcblxyXG4gICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpXHJcbiAgICAgIC5kYXR1bSh0ZXN0RGF0YSlcclxuICAgICAgLmNhbGwoY2hhcnQpXHJcblxyXG4gICAgYW5hbHl6ZVRpbWUoc2NvcGUuZGF0YSlcclxuXHJcbiAgICByZXR1cm5cclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmRpcmVjdGl2ZSAndGltZWxpbmUnLCAoJHN0YXRlKSAtPlxyXG4gIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lJyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIlxyXG5cclxuICBzY29wZTpcclxuICAgIHZlcnRpY2VzOiBcIj1cIlxyXG4gICAgam9iaWQ6IFwiPVwiXHJcblxyXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XHJcbiAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXVxyXG5cclxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcclxuICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKVxyXG5cclxuICAgIHRyYW5zbGF0ZUxhYmVsID0gKGxhYmVsKSAtPlxyXG4gICAgICBsYWJlbC5yZXBsYWNlKFwiJmd0O1wiLCBcIj5cIilcclxuXHJcbiAgICBhbmFseXplVGltZSA9IChkYXRhKSAtPlxyXG4gICAgICBkMy5zZWxlY3Qoc3ZnRWwpLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKClcclxuXHJcbiAgICAgIHRlc3REYXRhID0gW11cclxuXHJcbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLCAodmVydGV4KSAtPlxyXG4gICAgICAgIGlmIHZlcnRleFsnc3RhcnQtdGltZSddID4gLTFcclxuICAgICAgICAgIGlmIHZlcnRleC50eXBlIGlzICdzY2hlZHVsZWQnXHJcbiAgICAgICAgICAgIHRlc3REYXRhLnB1c2hcclxuICAgICAgICAgICAgICB0aW1lczogW1xyXG4gICAgICAgICAgICAgICAgbGFiZWw6IHRyYW5zbGF0ZUxhYmVsKHZlcnRleC5uYW1lKVxyXG4gICAgICAgICAgICAgICAgY29sb3I6IFwiI2NjY2NjY1wiXHJcbiAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1NTU1XCJcclxuICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZlcnRleFsnc3RhcnQtdGltZSddXHJcbiAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddXHJcbiAgICAgICAgICAgICAgICB0eXBlOiB2ZXJ0ZXgudHlwZVxyXG4gICAgICAgICAgICAgIF1cclxuICAgICAgICAgIGVsc2VcclxuICAgICAgICAgICAgdGVzdERhdGEucHVzaFxyXG4gICAgICAgICAgICAgIHRpbWVzOiBbXHJcbiAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpXHJcbiAgICAgICAgICAgICAgICBjb2xvcjogXCIjZDlmMWY3XCJcclxuICAgICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM2MmNkZWFcIlxyXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ11cclxuICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2ZXJ0ZXhbJ2VuZC10aW1lJ11cclxuICAgICAgICAgICAgICAgIGxpbms6IHZlcnRleC5pZFxyXG4gICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcclxuICAgICAgICAgICAgICBdXHJcblxyXG4gICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS5jbGljaygoZCwgaSwgZGF0dW0pIC0+XHJcbiAgICAgICAgaWYgZC5saW5rXHJcbiAgICAgICAgICAkc3RhdGUuZ28gXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7IGpvYmlkOiBzY29wZS5qb2JpZCwgdmVydGV4SWQ6IGQubGluayB9XHJcblxyXG4gICAgICApXHJcbiAgICAgIC50aWNrRm9ybWF0KHtcclxuICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIilcclxuICAgICAgICAjIHRpY2tUaW1lOiBkMy50aW1lLnNlY29uZFxyXG4gICAgICAgICMgdGlja0ludGVydmFsOiAwLjVcclxuICAgICAgICB0aWNrU2l6ZTogMVxyXG4gICAgICB9KVxyXG4gICAgICAucHJlZml4KFwibWFpblwiKVxyXG4gICAgICAubWFyZ2luKHsgbGVmdDogMCwgcmlnaHQ6IDAsIHRvcDogMCwgYm90dG9tOiAwIH0pXHJcbiAgICAgIC5pdGVtSGVpZ2h0KDMwKVxyXG4gICAgICAuc2hvd0JvcmRlckxpbmUoKVxyXG4gICAgICAuc2hvd0hvdXJUaW1lbGluZSgpXHJcblxyXG4gICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpXHJcbiAgICAgIC5kYXR1bSh0ZXN0RGF0YSlcclxuICAgICAgLmNhbGwoY2hhcnQpXHJcblxyXG4gICAgc2NvcGUuJHdhdGNoIGF0dHJzLnZlcnRpY2VzLCAoZGF0YSkgLT5cclxuICAgICAgYW5hbHl6ZVRpbWUoZGF0YSkgaWYgZGF0YVxyXG5cclxuICAgIHJldHVyblxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcbi5kaXJlY3RpdmUgJ3NwbGl0JywgKCkgLT4gXHJcbiAgcmV0dXJuIGNvbXBpbGU6ICh0RWxlbSwgdEF0dHJzKSAtPlxyXG4gICAgICBTcGxpdCh0RWxlbS5jaGlsZHJlbigpLCAoXHJcbiAgICAgICAgc2l6ZXM6IFs1MCwgNTBdXHJcbiAgICAgICAgZGlyZWN0aW9uOiAndmVydGljYWwnXHJcbiAgICAgICkpXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmRpcmVjdGl2ZSAnam9iUGxhbicsICgkdGltZW91dCkgLT5cclxuICB0ZW1wbGF0ZTogXCJcclxuICAgIDxzdmcgY2xhc3M9J2dyYXBoJyB3aWR0aD0nNTAwJyBoZWlnaHQ9JzQwMCc+PGcgLz48L3N2Zz5cclxuICAgIDxzdmcgY2xhc3M9J3RtcCcgd2lkdGg9JzEnIGhlaWdodD0nMSc+PGcgLz48L3N2Zz5cclxuICAgIDxkaXYgY2xhc3M9J2J0bi1ncm91cCB6b29tLWJ1dHRvbnMnPlxyXG4gICAgICA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20taW4nIG5nLWNsaWNrPSd6b29tSW4oKSc+PGkgY2xhc3M9J2ZhIGZhLXBsdXMnIC8+PC9hPlxyXG4gICAgICA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20tb3V0JyBuZy1jbGljaz0nem9vbU91dCgpJz48aSBjbGFzcz0nZmEgZmEtbWludXMnIC8+PC9hPlxyXG4gICAgPC9kaXY+XCJcclxuXHJcbiAgc2NvcGU6XHJcbiAgICBwbGFuOiAnPSdcclxuICAgIHNldE5vZGU6ICcmJ1xyXG5cclxuICBsaW5rOiAoc2NvcGUsIGVsZW0sIGF0dHJzKSAtPlxyXG4gICAgZyA9IG51bGxcclxuICAgIG1haW5ab29tID0gZDMuYmVoYXZpb3Iuem9vbSgpXHJcbiAgICBzdWJncmFwaHMgPSBbXVxyXG4gICAgam9iaWQgPSBhdHRycy5qb2JpZFxyXG5cclxuICAgIG1haW5TdmdFbGVtZW50ID0gZWxlbS5jaGlsZHJlbigpWzBdXHJcbiAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdXHJcbiAgICBtYWluVG1wRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVsxXVxyXG5cclxuICAgIGQzbWFpblN2ZyA9IGQzLnNlbGVjdChtYWluU3ZnRWxlbWVudClcclxuICAgIGQzbWFpblN2Z0cgPSBkMy5zZWxlY3QobWFpbkcpXHJcbiAgICBkM3RtcFN2ZyA9IGQzLnNlbGVjdChtYWluVG1wRWxlbWVudClcclxuXHJcbiAgICAjIGFuZ3VsYXIuZWxlbWVudChtYWluRykuZW1wdHkoKVxyXG4gICAgIyBkM21haW5TdmdHLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKClcclxuXHJcbiAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpXHJcbiAgICBhbmd1bGFyLmVsZW1lbnQoZWxlbS5jaGlsZHJlbigpWzBdKS53aWR0aChjb250YWluZXJXKVxyXG5cclxuICAgIHNjb3BlLnpvb21JbiA9IC0+XHJcbiAgICAgIGlmIG1haW5ab29tLnNjYWxlKCkgPCAyLjk5XHJcblxyXG4gICAgICAgICMgQ2FsY3VsYXRlIGFuZCBzdG9yZSBuZXcgdmFsdWVzIGluIHpvb20gb2JqZWN0XHJcbiAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKClcclxuICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxyXG4gICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXHJcbiAgICAgICAgbWFpblpvb20uc2NhbGUgbWFpblpvb20uc2NhbGUoKSArIDAuMVxyXG4gICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZSBbIHYxLCB2MiBdXHJcblxyXG4gICAgICAgICMgVHJhbnNmb3JtIHN2Z1xyXG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIlxyXG5cclxuICAgIHNjb3BlLnpvb21PdXQgPSAtPlxyXG4gICAgICBpZiBtYWluWm9vbS5zY2FsZSgpID4gMC4zMVxyXG5cclxuICAgICAgICAjIENhbGN1bGF0ZSBhbmQgc3RvcmUgbmV3IHZhbHVlcyBpbiBtYWluWm9vbSBvYmplY3RcclxuICAgICAgICBtYWluWm9vbS5zY2FsZSBtYWluWm9vbS5zY2FsZSgpIC0gMC4xXHJcbiAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKClcclxuICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxyXG4gICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXHJcbiAgICAgICAgbWFpblpvb20udHJhbnNsYXRlIFsgdjEsIHYyIF1cclxuXHJcbiAgICAgICAgIyBUcmFuc2Zvcm0gc3ZnXHJcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiXHJcblxyXG4gICAgI2NyZWF0ZSBhIGxhYmVsIG9mIGFuIGVkZ2VcclxuICAgIGNyZWF0ZUxhYmVsRWRnZSA9IChlbCkgLT5cclxuICAgICAgbGFiZWxWYWx1ZSA9IFwiXCJcclxuICAgICAgaWYgZWwuc2hpcF9zdHJhdGVneT8gb3IgZWwubG9jYWxfc3RyYXRlZ3k/XHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxkaXYgY2xhc3M9J2VkZ2UtbGFiZWwnPlwiXHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBlbC5zaGlwX3N0cmF0ZWd5ICBpZiBlbC5zaGlwX3N0cmF0ZWd5P1xyXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCIgKFwiICsgZWwudGVtcF9tb2RlICsgXCIpXCIgIHVubGVzcyBlbC50ZW1wX21vZGUgaXMgYHVuZGVmaW5lZGBcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiLDxicj5cIiArIGVsLmxvY2FsX3N0cmF0ZWd5ICB1bmxlc3MgZWwubG9jYWxfc3RyYXRlZ3kgaXMgYHVuZGVmaW5lZGBcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCJcclxuICAgICAgbGFiZWxWYWx1ZVxyXG5cclxuXHJcbiAgICAjIHRydWUsIGlmIHRoZSBub2RlIGlzIGEgc3BlY2lhbCBub2RlIGZyb20gYW4gaXRlcmF0aW9uXHJcbiAgICBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlID0gKGluZm8pIC0+XHJcbiAgICAgIChpbmZvIGlzIFwicGFydGlhbFNvbHV0aW9uXCIgb3IgaW5mbyBpcyBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiBvciBpbmZvIGlzIFwid29ya3NldFwiIG9yIGluZm8gaXMgXCJuZXh0V29ya3NldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvblNldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvbkRlbHRhXCIpXHJcblxyXG4gICAgZ2V0Tm9kZVR5cGUgPSAoZWwsIGluZm8pIC0+XHJcbiAgICAgIGlmIGluZm8gaXMgXCJtaXJyb3JcIlxyXG4gICAgICAgICdub2RlLW1pcnJvcidcclxuXHJcbiAgICAgIGVsc2UgaWYgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKVxyXG4gICAgICAgICdub2RlLWl0ZXJhdGlvbidcclxuXHJcbiAgICAgIGVsc2VcclxuICAgICAgICAnbm9kZS1ub3JtYWwnXHJcblxyXG4gICAgIyBjcmVhdGVzIHRoZSBsYWJlbCBvZiBhIG5vZGUsIGluIGluZm8gaXMgc3RvcmVkLCB3aGV0aGVyIGl0IGlzIGEgc3BlY2lhbCBub2RlIChsaWtlIGEgbWlycm9yIGluIGFuIGl0ZXJhdGlvbilcclxuICAgIGNyZWF0ZUxhYmVsTm9kZSA9IChlbCwgaW5mbywgbWF4VywgbWF4SCkgLT5cclxuICAgICAgIyBsYWJlbFZhbHVlID0gXCI8YSBocmVmPScjL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRleC9cIiArIGVsLmlkICsgXCInIGNsYXNzPSdub2RlLWxhYmVsIFwiICsgZ2V0Tm9kZVR5cGUoZWwsIGluZm8pICsgXCInPlwiXHJcbiAgICAgIGxhYmVsVmFsdWUgPSBcIjxkaXYgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0ZXgvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIlxyXG5cclxuICAgICAgIyBOb2RlbmFtZVxyXG4gICAgICBpZiBpbmZvIGlzIFwibWlycm9yXCJcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPk1pcnJvciBvZiBcIiArIGVsLm9wZXJhdG9yICsgXCI8L2gzPlwiXHJcbiAgICAgIGVsc2VcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCJcclxuICAgICAgaWYgZWwuZGVzY3JpcHRpb24gaXMgXCJcIlxyXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCJcIlxyXG4gICAgICBlbHNlXHJcbiAgICAgICAgc3RlcE5hbWUgPSBlbC5kZXNjcmlwdGlvblxyXG5cclxuICAgICAgICAjIGNsZWFuIHN0ZXBOYW1lXHJcbiAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKVxyXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDQgY2xhc3M9J3N0ZXAtbmFtZSc+XCIgKyBzdGVwTmFtZSArIFwiPC9oND5cIlxyXG5cclxuICAgICAgIyBJZiB0aGlzIG5vZGUgaXMgYW4gXCJpdGVyYXRpb25cIiB3ZSBuZWVkIGEgZGlmZmVyZW50IHBhbmVsLWJvZHlcclxuICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cclxuICAgICAgICBsYWJlbFZhbHVlICs9IGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbihlbC5pZCwgbWF4VywgbWF4SClcclxuICAgICAgZWxzZVxyXG5cclxuICAgICAgICAjIE90aGVyd2lzZSBhZGQgaW5mb3NcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlwiICsgaW5mbyArIFwiIE5vZGU8L2g1PlwiICBpZiBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pXHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5QYXJhbGxlbGlzbTogXCIgKyBlbC5wYXJhbGxlbGlzbSArIFwiPC9oNT5cIiAgdW5sZXNzIGVsLnBhcmFsbGVsaXNtIGlzIFwiXCJcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1Pk9wZXJhdGlvbjogXCIgKyBzaG9ydGVuU3RyaW5nKGVsLm9wZXJhdG9yX3N0cmF0ZWd5KSArIFwiPC9oNT5cIiB1bmxlc3MgZWwub3BlcmF0b3IgaXMgYHVuZGVmaW5lZGAgb3Igbm90IGVsLm9wZXJhdG9yX3N0cmF0ZWd5XHJcbiAgICAgICMgbGFiZWxWYWx1ZSArPSBcIjwvYT5cIlxyXG4gICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCJcclxuICAgICAgbGFiZWxWYWx1ZVxyXG5cclxuICAgICMgRXh0ZW5kcyB0aGUgbGFiZWwgb2YgYSBub2RlIHdpdGggYW4gYWRkaXRpb25hbCBzdmcgRWxlbWVudCB0byBwcmVzZW50IHRoZSBpdGVyYXRpb24uXHJcbiAgICBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24gPSAoaWQsIG1heFcsIG1heEgpIC0+XHJcbiAgICAgIHN2Z0lEID0gXCJzdmctXCIgKyBpZFxyXG5cclxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPHN2ZyBjbGFzcz0nXCIgKyBzdmdJRCArIFwiJyB3aWR0aD1cIiArIG1heFcgKyBcIiBoZWlnaHQ9XCIgKyBtYXhIICsgXCI+PGcgLz48L3N2Zz5cIlxyXG4gICAgICBsYWJlbFZhbHVlXHJcblxyXG4gICAgIyBTcGxpdCBhIHN0cmluZyBpbnRvIG11bHRpcGxlIGxpbmVzIHNvIHRoYXQgZWFjaCBsaW5lIGhhcyBsZXNzIHRoYW4gMzAgbGV0dGVycy5cclxuICAgIHNob3J0ZW5TdHJpbmcgPSAocykgLT5cclxuICAgICAgIyBtYWtlIHN1cmUgdGhhdCBuYW1lIGRvZXMgbm90IGNvbnRhaW4gYSA8IChiZWNhdXNlIG9mIGh0bWwpXHJcbiAgICAgIGlmIHMuY2hhckF0KDApIGlzIFwiPFwiXHJcbiAgICAgICAgcyA9IHMucmVwbGFjZShcIjxcIiwgXCImbHQ7XCIpXHJcbiAgICAgICAgcyA9IHMucmVwbGFjZShcIj5cIiwgXCImZ3Q7XCIpXHJcbiAgICAgIHNiciA9IFwiXCJcclxuICAgICAgd2hpbGUgcy5sZW5ndGggPiAzMFxyXG4gICAgICAgIHNiciA9IHNiciArIHMuc3Vic3RyaW5nKDAsIDMwKSArIFwiPGJyPlwiXHJcbiAgICAgICAgcyA9IHMuc3Vic3RyaW5nKDMwLCBzLmxlbmd0aClcclxuICAgICAgc2JyID0gc2JyICsgc1xyXG4gICAgICBzYnJcclxuXHJcbiAgICBjcmVhdGVOb2RlID0gKGcsIGRhdGEsIGVsLCBpc1BhcmVudCA9IGZhbHNlLCBtYXhXLCBtYXhIKSAtPlxyXG4gICAgICAjIGNyZWF0ZSBub2RlLCBzZW5kIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25zIGFib3V0IHRoZSBub2RlIGlmIGl0IGlzIGEgc3BlY2lhbCBvbmVcclxuICAgICAgaWYgZWwuaWQgaXMgZGF0YS5wYXJ0aWFsX3NvbHV0aW9uXHJcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxyXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SClcclxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXHJcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIpXHJcblxyXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEubmV4dF9wYXJ0aWFsX3NvbHV0aW9uXHJcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxyXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpXHJcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xyXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIilcclxuXHJcbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS53b3Jrc2V0XHJcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxyXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJ3b3Jrc2V0XCIsIG1heFcsIG1heEgpXHJcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xyXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcIndvcmtzZXRcIilcclxuXHJcbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5uZXh0X3dvcmtzZXRcclxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXHJcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRXb3Jrc2V0XCIsIG1heFcsIG1heEgpXHJcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xyXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRXb3Jrc2V0XCIpXHJcblxyXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEuc29sdXRpb25fc2V0XHJcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxyXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvblNldFwiLCBtYXhXLCBtYXhIKVxyXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcclxuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvblNldFwiKVxyXG5cclxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLnNvbHV0aW9uX2RlbHRhXHJcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxyXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIsIG1heFcsIG1heEgpXHJcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xyXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uRGVsdGFcIilcclxuXHJcbiAgICAgIGVsc2VcclxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXHJcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIlwiLCBtYXhXLCBtYXhIKVxyXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcclxuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJcIilcclxuXHJcbiAgICBjcmVhdGVFZGdlID0gKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKSAtPlxyXG4gICAgICBnLnNldEVkZ2UgcHJlZC5pZCwgZWwuaWQsXHJcbiAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShwcmVkKVxyXG4gICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXHJcbiAgICAgICAgYXJyb3doZWFkOiAnbm9ybWFsJ1xyXG5cclxuICAgIGxvYWRKc29uVG9EYWdyZSA9IChnLCBkYXRhKSAtPlxyXG4gICAgICBleGlzdGluZ05vZGVzID0gW11cclxuXHJcbiAgICAgIGlmIGRhdGEubm9kZXM/XHJcbiAgICAgICAgIyBUaGlzIGlzIHRoZSBub3JtYWwganNvbiBkYXRhXHJcbiAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5ub2Rlc1xyXG5cclxuICAgICAgZWxzZVxyXG4gICAgICAgICMgVGhpcyBpcyBhbiBpdGVyYXRpb24sIHdlIG5vdyBzdG9yZSBzcGVjaWFsIGl0ZXJhdGlvbiBub2RlcyBpZiBwb3NzaWJsZVxyXG4gICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEuc3RlcF9mdW5jdGlvblxyXG4gICAgICAgIGlzUGFyZW50ID0gdHJ1ZVxyXG5cclxuICAgICAgZm9yIGVsIGluIHRvSXRlcmF0ZVxyXG4gICAgICAgIG1heFcgPSAwXHJcbiAgICAgICAgbWF4SCA9IDBcclxuXHJcbiAgICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvblxyXG4gICAgICAgICAgc2cgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7IG11bHRpZ3JhcGg6IHRydWUsIGNvbXBvdW5kOiB0cnVlIH0pLnNldEdyYXBoKHtcclxuICAgICAgICAgICAgbm9kZXNlcDogMjBcclxuICAgICAgICAgICAgZWRnZXNlcDogMFxyXG4gICAgICAgICAgICByYW5rc2VwOiAyMFxyXG4gICAgICAgICAgICByYW5rZGlyOiBcIkxSXCJcclxuICAgICAgICAgICAgbWFyZ2lueDogMTBcclxuICAgICAgICAgICAgbWFyZ2lueTogMTBcclxuICAgICAgICAgICAgfSlcclxuXHJcbiAgICAgICAgICBzdWJncmFwaHNbZWwuaWRdID0gc2dcclxuXHJcbiAgICAgICAgICBsb2FkSnNvblRvRGFncmUoc2csIGVsKVxyXG5cclxuICAgICAgICAgIHIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxyXG4gICAgICAgICAgZDN0bXBTdmcuc2VsZWN0KCdnJykuY2FsbChyLCBzZylcclxuICAgICAgICAgIG1heFcgPSBzZy5ncmFwaCgpLndpZHRoXHJcbiAgICAgICAgICBtYXhIID0gc2cuZ3JhcGgoKS5oZWlnaHRcclxuXHJcbiAgICAgICAgICBhbmd1bGFyLmVsZW1lbnQobWFpblRtcEVsZW1lbnQpLmVtcHR5KClcclxuXHJcbiAgICAgICAgY3JlYXRlTm9kZShnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpXHJcblxyXG4gICAgICAgIGV4aXN0aW5nTm9kZXMucHVzaCBlbC5pZFxyXG5cclxuICAgICAgICAjIGNyZWF0ZSBlZGdlcyBmcm9tIGlucHV0cyB0byBjdXJyZW50IG5vZGVcclxuICAgICAgICBpZiBlbC5pbnB1dHM/XHJcbiAgICAgICAgICBmb3IgcHJlZCBpbiBlbC5pbnB1dHNcclxuICAgICAgICAgICAgY3JlYXRlRWRnZShnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZClcclxuXHJcbiAgICAgIGdcclxuXHJcbiAgICAjIHNlYXJjaGVzIGluIHRoZSBnbG9iYWwgSlNPTkRhdGEgZm9yIHRoZSBub2RlIHdpdGggdGhlIGdpdmVuIGlkXHJcbiAgICBzZWFyY2hGb3JOb2RlID0gKGRhdGEsIG5vZGVJRCkgLT5cclxuICAgICAgZm9yIGkgb2YgZGF0YS5ub2Rlc1xyXG4gICAgICAgIGVsID0gZGF0YS5ub2Rlc1tpXVxyXG4gICAgICAgIHJldHVybiBlbCAgaWYgZWwuaWQgaXMgbm9kZUlEXHJcblxyXG4gICAgICAgICMgbG9vayBmb3Igbm9kZXMgdGhhdCBhcmUgaW4gaXRlcmF0aW9uc1xyXG4gICAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb24/XHJcbiAgICAgICAgICBmb3IgaiBvZiBlbC5zdGVwX2Z1bmN0aW9uXHJcbiAgICAgICAgICAgIHJldHVybiBlbC5zdGVwX2Z1bmN0aW9uW2pdICBpZiBlbC5zdGVwX2Z1bmN0aW9uW2pdLmlkIGlzIG5vZGVJRFxyXG5cclxuICAgIGRyYXdHcmFwaCA9IChkYXRhKSAtPlxyXG4gICAgICBnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoeyBtdWx0aWdyYXBoOiB0cnVlLCBjb21wb3VuZDogdHJ1ZSB9KS5zZXRHcmFwaCh7XHJcbiAgICAgICAgbm9kZXNlcDogNzBcclxuICAgICAgICBlZGdlc2VwOiAwXHJcbiAgICAgICAgcmFua3NlcDogNTBcclxuICAgICAgICByYW5rZGlyOiBcIkxSXCJcclxuICAgICAgICBtYXJnaW54OiA0MFxyXG4gICAgICAgIG1hcmdpbnk6IDQwXHJcbiAgICAgICAgfSlcclxuXHJcbiAgICAgIGxvYWRKc29uVG9EYWdyZShnLCBkYXRhKVxyXG5cclxuICAgICAgcmVuZGVyZXIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxyXG4gICAgICBkM21haW5TdmdHLmNhbGwocmVuZGVyZXIsIGcpXHJcblxyXG4gICAgICBmb3IgaSwgc2cgb2Ygc3ViZ3JhcGhzXHJcbiAgICAgICAgZDNtYWluU3ZnLnNlbGVjdCgnc3ZnLnN2Zy0nICsgaSArICcgZycpLmNhbGwocmVuZGVyZXIsIHNnKVxyXG5cclxuICAgICAgbmV3U2NhbGUgPSAwLjVcclxuXHJcbiAgICAgIHhDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLndpZHRoKCkgLSBnLmdyYXBoKCkud2lkdGggKiBuZXdTY2FsZSkgLyAyKVxyXG4gICAgICB5Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS5oZWlnaHQoKSAtIGcuZ3JhcGgoKS5oZWlnaHQgKiBuZXdTY2FsZSkgLyAyKVxyXG5cclxuICAgICAgbWFpblpvb20uc2NhbGUobmV3U2NhbGUpLnRyYW5zbGF0ZShbeENlbnRlck9mZnNldCwgeUNlbnRlck9mZnNldF0pXHJcblxyXG4gICAgICBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB4Q2VudGVyT2Zmc2V0ICsgXCIsIFwiICsgeUNlbnRlck9mZnNldCArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIilcclxuXHJcbiAgICAgIG1haW5ab29tLm9uKFwiem9vbVwiLCAtPlxyXG4gICAgICAgIGV2ID0gZDMuZXZlbnRcclxuICAgICAgICBkM21haW5TdmdHLmF0dHIgXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBldi50cmFuc2xhdGUgKyBcIikgc2NhbGUoXCIgKyBldi5zY2FsZSArIFwiKVwiXHJcbiAgICAgIClcclxuICAgICAgbWFpblpvb20oZDNtYWluU3ZnKVxyXG5cclxuICAgICAgZDNtYWluU3ZnRy5zZWxlY3RBbGwoJy5ub2RlJykub24gJ2NsaWNrJywgKGQpIC0+XHJcbiAgICAgICAgc2NvcGUuc2V0Tm9kZSh7IG5vZGVpZDogZCB9KVxyXG5cclxuICAgIHNjb3BlLiR3YXRjaCBhdHRycy5wbGFuLCAobmV3UGxhbikgLT5cclxuICAgICAgZHJhd0dyYXBoKG5ld1BsYW4pIGlmIG5ld1BsYW5cclxuXHJcbiAgICByZXR1cm5cclxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuZGlyZWN0aXZlKCd2ZXJ0ZXgnLCBmdW5jdGlvbigkc3RhdGUpIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZSBzZWNvbmRhcnknIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiLFxuICAgIHNjb3BlOiB7XG4gICAgICBkYXRhOiBcIj1cIlxuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgYW5hbHl6ZVRpbWUsIGNvbnRhaW5lclcsIHN2Z0VsO1xuICAgICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpO1xuICAgICAgYW5hbHl6ZVRpbWUgPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBjaGFydCwgc3ZnLCB0ZXN0RGF0YTtcbiAgICAgICAgZDMuc2VsZWN0KHN2Z0VsKS5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpO1xuICAgICAgICB0ZXN0RGF0YSA9IFtdO1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YS5zdWJ0YXNrcywgZnVuY3Rpb24oc3VidGFzaywgaSkge1xuICAgICAgICAgIHZhciB0aW1lcztcbiAgICAgICAgICB0aW1lcyA9IFtcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiU2NoZWR1bGVkXCIsXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiM2NjZcIixcbiAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiLFxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJTQ0hFRFVMRURcIl0sXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl0sXG4gICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgfSwge1xuICAgICAgICAgICAgICBsYWJlbDogXCJEZXBsb3lpbmdcIixcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2FhYVwiLFxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXSxcbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl0sXG4gICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgfVxuICAgICAgICAgIF07XG4gICAgICAgICAgaWYgKHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdID4gMCkge1xuICAgICAgICAgICAgdGltZXMucHVzaCh7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIlJ1bm5pbmdcIixcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2RkZFwiLFxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl0sXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJGSU5JU0hFRFwiXSxcbiAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgbGFiZWw6IFwiKFwiICsgc3VidGFzay5zdWJ0YXNrICsgXCIpIFwiICsgc3VidGFzay5ob3N0LFxuICAgICAgICAgICAgdGltZXM6IHRpbWVzXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0pO1xuICAgICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS50aWNrRm9ybWF0KHtcbiAgICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIiksXG4gICAgICAgICAgdGlja1NpemU6IDFcbiAgICAgICAgfSkucHJlZml4KFwic2luZ2xlXCIpLmxhYmVsRm9ybWF0KGZ1bmN0aW9uKGxhYmVsKSB7XG4gICAgICAgICAgcmV0dXJuIGxhYmVsO1xuICAgICAgICB9KS5tYXJnaW4oe1xuICAgICAgICAgIGxlZnQ6IDEwMCxcbiAgICAgICAgICByaWdodDogMCxcbiAgICAgICAgICB0b3A6IDAsXG4gICAgICAgICAgYm90dG9tOiAwXG4gICAgICAgIH0pLml0ZW1IZWlnaHQoMzApLnJlbGF0aXZlVGltZSgpO1xuICAgICAgICByZXR1cm4gc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKS5kYXR1bSh0ZXN0RGF0YSkuY2FsbChjaGFydCk7XG4gICAgICB9O1xuICAgICAgYW5hbHl6ZVRpbWUoc2NvcGUuZGF0YSk7XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCd0aW1lbGluZScsIGZ1bmN0aW9uKCRzdGF0ZSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lJyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIixcbiAgICBzY29wZToge1xuICAgICAgdmVydGljZXM6IFwiPVwiLFxuICAgICAgam9iaWQ6IFwiPVwiXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBhbmFseXplVGltZSwgY29udGFpbmVyVywgc3ZnRWwsIHRyYW5zbGF0ZUxhYmVsO1xuICAgICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpO1xuICAgICAgdHJhbnNsYXRlTGFiZWwgPSBmdW5jdGlvbihsYWJlbCkge1xuICAgICAgICByZXR1cm4gbGFiZWwucmVwbGFjZShcIiZndDtcIiwgXCI+XCIpO1xuICAgICAgfTtcbiAgICAgIGFuYWx5emVUaW1lID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgY2hhcnQsIHN2ZywgdGVzdERhdGE7XG4gICAgICAgIGQzLnNlbGVjdChzdmdFbCkuc2VsZWN0QWxsKFwiKlwiKS5yZW1vdmUoKTtcbiAgICAgICAgdGVzdERhdGEgPSBbXTtcbiAgICAgICAgYW5ndWxhci5mb3JFYWNoKGRhdGEsIGZ1bmN0aW9uKHZlcnRleCkge1xuICAgICAgICAgIGlmICh2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXSA+IC0xKSB7XG4gICAgICAgICAgICBpZiAodmVydGV4LnR5cGUgPT09ICdzY2hlZHVsZWQnKSB7XG4gICAgICAgICAgICAgIHJldHVybiB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpLFxuICAgICAgICAgICAgICAgICAgICBjb2xvcjogXCIjY2NjY2NjXCIsXG4gICAgICAgICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTU1NTVcIixcbiAgICAgICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2ZXJ0ZXhbJ2VuZC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIHR5cGU6IHZlcnRleC50eXBlXG4gICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgXVxuICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgIHJldHVybiB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpLFxuICAgICAgICAgICAgICAgICAgICBjb2xvcjogXCIjZDlmMWY3XCIsXG4gICAgICAgICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM2MmNkZWFcIixcbiAgICAgICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2ZXJ0ZXhbJ2VuZC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIGxpbms6IHZlcnRleC5pZCxcbiAgICAgICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcbiAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBdXG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLmNsaWNrKGZ1bmN0aW9uKGQsIGksIGRhdHVtKSB7XG4gICAgICAgICAgaWYgKGQubGluaykge1xuICAgICAgICAgICAgcmV0dXJuICRzdGF0ZS5nbyhcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHtcbiAgICAgICAgICAgICAgam9iaWQ6IHNjb3BlLmpvYmlkLFxuICAgICAgICAgICAgICB2ZXJ0ZXhJZDogZC5saW5rXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pLnRpY2tGb3JtYXQoe1xuICAgICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlTFwiKSxcbiAgICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgICB9KS5wcmVmaXgoXCJtYWluXCIpLm1hcmdpbih7XG4gICAgICAgICAgbGVmdDogMCxcbiAgICAgICAgICByaWdodDogMCxcbiAgICAgICAgICB0b3A6IDAsXG4gICAgICAgICAgYm90dG9tOiAwXG4gICAgICAgIH0pLml0ZW1IZWlnaHQoMzApLnNob3dCb3JkZXJMaW5lKCkuc2hvd0hvdXJUaW1lbGluZSgpO1xuICAgICAgICByZXR1cm4gc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKS5kYXR1bSh0ZXN0RGF0YSkuY2FsbChjaGFydCk7XG4gICAgICB9O1xuICAgICAgc2NvcGUuJHdhdGNoKGF0dHJzLnZlcnRpY2VzLCBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIGlmIChkYXRhKSB7XG4gICAgICAgICAgcmV0dXJuIGFuYWx5emVUaW1lKGRhdGEpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ3NwbGl0JywgZnVuY3Rpb24oKSB7XG4gIHJldHVybiB7XG4gICAgY29tcGlsZTogZnVuY3Rpb24odEVsZW0sIHRBdHRycykge1xuICAgICAgcmV0dXJuIFNwbGl0KHRFbGVtLmNoaWxkcmVuKCksIHtcbiAgICAgICAgc2l6ZXM6IFs1MCwgNTBdLFxuICAgICAgICBkaXJlY3Rpb246ICd2ZXJ0aWNhbCdcbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgnam9iUGxhbicsIGZ1bmN0aW9uKCR0aW1lb3V0KSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0nZ3JhcGgnIHdpZHRoPSc1MDAnIGhlaWdodD0nNDAwJz48ZyAvPjwvc3ZnPiA8c3ZnIGNsYXNzPSd0bXAnIHdpZHRoPScxJyBoZWlnaHQ9JzEnPjxnIC8+PC9zdmc+IDxkaXYgY2xhc3M9J2J0bi1ncm91cCB6b29tLWJ1dHRvbnMnPiA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20taW4nIG5nLWNsaWNrPSd6b29tSW4oKSc+PGkgY2xhc3M9J2ZhIGZhLXBsdXMnIC8+PC9hPiA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20tb3V0JyBuZy1jbGljaz0nem9vbU91dCgpJz48aSBjbGFzcz0nZmEgZmEtbWludXMnIC8+PC9hPiA8L2Rpdj5cIixcbiAgICBzY29wZToge1xuICAgICAgcGxhbjogJz0nLFxuICAgICAgc2V0Tm9kZTogJyYnXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBjb250YWluZXJXLCBjcmVhdGVFZGdlLCBjcmVhdGVMYWJlbEVkZ2UsIGNyZWF0ZUxhYmVsTm9kZSwgY3JlYXRlTm9kZSwgZDNtYWluU3ZnLCBkM21haW5TdmdHLCBkM3RtcFN2ZywgZHJhd0dyYXBoLCBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24sIGcsIGdldE5vZGVUeXBlLCBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlLCBqb2JpZCwgbG9hZEpzb25Ub0RhZ3JlLCBtYWluRywgbWFpblN2Z0VsZW1lbnQsIG1haW5UbXBFbGVtZW50LCBtYWluWm9vbSwgc2VhcmNoRm9yTm9kZSwgc2hvcnRlblN0cmluZywgc3ViZ3JhcGhzO1xuICAgICAgZyA9IG51bGw7XG4gICAgICBtYWluWm9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKTtcbiAgICAgIHN1YmdyYXBocyA9IFtdO1xuICAgICAgam9iaWQgPSBhdHRycy5qb2JpZDtcbiAgICAgIG1haW5TdmdFbGVtZW50ID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgbWFpbkcgPSBlbGVtLmNoaWxkcmVuKCkuY2hpbGRyZW4oKVswXTtcbiAgICAgIG1haW5UbXBFbGVtZW50ID0gZWxlbS5jaGlsZHJlbigpWzFdO1xuICAgICAgZDNtYWluU3ZnID0gZDMuc2VsZWN0KG1haW5TdmdFbGVtZW50KTtcbiAgICAgIGQzbWFpblN2Z0cgPSBkMy5zZWxlY3QobWFpbkcpO1xuICAgICAgZDN0bXBTdmcgPSBkMy5zZWxlY3QobWFpblRtcEVsZW1lbnQpO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChlbGVtLmNoaWxkcmVuKClbMF0pLndpZHRoKGNvbnRhaW5lclcpO1xuICAgICAgc2NvcGUuem9vbUluID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciB0cmFuc2xhdGUsIHYxLCB2MjtcbiAgICAgICAgaWYgKG1haW5ab29tLnNjYWxlKCkgPCAyLjk5KSB7XG4gICAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKCk7XG4gICAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgbWFpblpvb20uc2NhbGUobWFpblpvb20uc2NhbGUoKSArIDAuMSk7XG4gICAgICAgICAgbWFpblpvb20udHJhbnNsYXRlKFt2MSwgdjJdKTtcbiAgICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIHNjb3BlLnpvb21PdXQgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHRyYW5zbGF0ZSwgdjEsIHYyO1xuICAgICAgICBpZiAobWFpblpvb20uc2NhbGUoKSA+IDAuMzEpIHtcbiAgICAgICAgICBtYWluWm9vbS5zY2FsZShtYWluWm9vbS5zY2FsZSgpIC0gMC4xKTtcbiAgICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKTtcbiAgICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUoW3YxLCB2Ml0pO1xuICAgICAgICAgIHJldHVybiBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgY3JlYXRlTGFiZWxFZGdlID0gZnVuY3Rpb24oZWwpIHtcbiAgICAgICAgdmFyIGxhYmVsVmFsdWU7XG4gICAgICAgIGxhYmVsVmFsdWUgPSBcIlwiO1xuICAgICAgICBpZiAoKGVsLnNoaXBfc3RyYXRlZ3kgIT0gbnVsbCkgfHwgKGVsLmxvY2FsX3N0cmF0ZWd5ICE9IG51bGwpKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxkaXYgY2xhc3M9J2VkZ2UtbGFiZWwnPlwiO1xuICAgICAgICAgIGlmIChlbC5zaGlwX3N0cmF0ZWd5ICE9IG51bGwpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gZWwuc2hpcF9zdHJhdGVneTtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLnRlbXBfbW9kZSAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiIChcIiArIGVsLnRlbXBfbW9kZSArIFwiKVwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwubG9jYWxfc3RyYXRlZ3kgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiw8YnI+XCIgKyBlbC5sb2NhbF9zdHJhdGVneTtcbiAgICAgICAgICB9XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvZGl2PlwiO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBsYWJlbFZhbHVlO1xuICAgICAgfTtcbiAgICAgIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUgPSBmdW5jdGlvbihpbmZvKSB7XG4gICAgICAgIHJldHVybiBpbmZvID09PSBcInBhcnRpYWxTb2x1dGlvblwiIHx8IGluZm8gPT09IFwibmV4dFBhcnRpYWxTb2x1dGlvblwiIHx8IGluZm8gPT09IFwid29ya3NldFwiIHx8IGluZm8gPT09IFwibmV4dFdvcmtzZXRcIiB8fCBpbmZvID09PSBcInNvbHV0aW9uU2V0XCIgfHwgaW5mbyA9PT0gXCJzb2x1dGlvbkRlbHRhXCI7XG4gICAgICB9O1xuICAgICAgZ2V0Tm9kZVR5cGUgPSBmdW5jdGlvbihlbCwgaW5mbykge1xuICAgICAgICBpZiAoaW5mbyA9PT0gXCJtaXJyb3JcIikge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1taXJyb3InO1xuICAgICAgICB9IGVsc2UgaWYgKGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbykpIHtcbiAgICAgICAgICByZXR1cm4gJ25vZGUtaXRlcmF0aW9uJztcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gJ25vZGUtbm9ybWFsJztcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUxhYmVsTm9kZSA9IGZ1bmN0aW9uKGVsLCBpbmZvLCBtYXhXLCBtYXhIKSB7XG4gICAgICAgIHZhciBsYWJlbFZhbHVlLCBzdGVwTmFtZTtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiPGRpdiBocmVmPScjL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRleC9cIiArIGVsLmlkICsgXCInIGNsYXNzPSdub2RlLWxhYmVsIFwiICsgZ2V0Tm9kZVR5cGUoZWwsIGluZm8pICsgXCInPlwiO1xuICAgICAgICBpZiAoaW5mbyA9PT0gXCJtaXJyb3JcIikge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+TWlycm9yIG9mIFwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCI7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5cIiArIGVsLm9wZXJhdG9yICsgXCI8L2gzPlwiO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbC5kZXNjcmlwdGlvbiA9PT0gXCJcIikge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCJcIjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBzdGVwTmFtZSA9IGVsLmRlc2NyaXB0aW9uO1xuICAgICAgICAgIHN0ZXBOYW1lID0gc2hvcnRlblN0cmluZyhzdGVwTmFtZSk7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNCBjbGFzcz0nc3RlcC1uYW1lJz5cIiArIHN0ZXBOYW1lICsgXCI8L2g0PlwiO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uICE9IG51bGwpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbihlbC5pZCwgbWF4VywgbWF4SCk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgaWYgKGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbykpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+XCIgKyBpbmZvICsgXCIgTm9kZTwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5wYXJhbGxlbGlzbSAhPT0gXCJcIikge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5QYXJhbGxlbGlzbTogXCIgKyBlbC5wYXJhbGxlbGlzbSArIFwiPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKCEoZWwub3BlcmF0b3IgPT09IHVuZGVmaW5lZCB8fCAhZWwub3BlcmF0b3Jfc3RyYXRlZ3kpKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1Pk9wZXJhdGlvbjogXCIgKyBzaG9ydGVuU3RyaW5nKGVsLm9wZXJhdG9yX3N0cmF0ZWd5KSArIFwiPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvZGl2PlwiO1xuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24gPSBmdW5jdGlvbihpZCwgbWF4VywgbWF4SCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZSwgc3ZnSUQ7XG4gICAgICAgIHN2Z0lEID0gXCJzdmctXCIgKyBpZDtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiPHN2ZyBjbGFzcz0nXCIgKyBzdmdJRCArIFwiJyB3aWR0aD1cIiArIG1heFcgKyBcIiBoZWlnaHQ9XCIgKyBtYXhIICsgXCI+PGcgLz48L3N2Zz5cIjtcbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgc2hvcnRlblN0cmluZyA9IGZ1bmN0aW9uKHMpIHtcbiAgICAgICAgdmFyIHNicjtcbiAgICAgICAgaWYgKHMuY2hhckF0KDApID09PSBcIjxcIikge1xuICAgICAgICAgIHMgPSBzLnJlcGxhY2UoXCI8XCIsIFwiJmx0O1wiKTtcbiAgICAgICAgICBzID0gcy5yZXBsYWNlKFwiPlwiLCBcIiZndDtcIik7XG4gICAgICAgIH1cbiAgICAgICAgc2JyID0gXCJcIjtcbiAgICAgICAgd2hpbGUgKHMubGVuZ3RoID4gMzApIHtcbiAgICAgICAgICBzYnIgPSBzYnIgKyBzLnN1YnN0cmluZygwLCAzMCkgKyBcIjxicj5cIjtcbiAgICAgICAgICBzID0gcy5zdWJzdHJpbmcoMzAsIHMubGVuZ3RoKTtcbiAgICAgICAgfVxuICAgICAgICBzYnIgPSBzYnIgKyBzO1xuICAgICAgICByZXR1cm4gc2JyO1xuICAgICAgfTtcbiAgICAgIGNyZWF0ZU5vZGUgPSBmdW5jdGlvbihnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpIHtcbiAgICAgICAgaWYgKGlzUGFyZW50ID09IG51bGwpIHtcbiAgICAgICAgICBpc1BhcmVudCA9IGZhbHNlO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbC5pZCA9PT0gZGF0YS5wYXJ0aWFsX3NvbHV0aW9uKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEubmV4dF9wYXJ0aWFsX3NvbHV0aW9uKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS53b3Jrc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJ3b3Jrc2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIndvcmtzZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5uZXh0X3dvcmtzZXQpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRXb3Jrc2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRXb3Jrc2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEuc29sdXRpb25fc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvblNldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvblNldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLnNvbHV0aW9uX2RlbHRhKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uRGVsdGFcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIlwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUVkZ2UgPSBmdW5jdGlvbihnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCkge1xuICAgICAgICByZXR1cm4gZy5zZXRFZGdlKHByZWQuaWQsIGVsLmlkLCB7XG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShwcmVkKSxcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICBhcnJvd2hlYWQ6ICdub3JtYWwnXG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICAgIGxvYWRKc29uVG9EYWdyZSA9IGZ1bmN0aW9uKGcsIGRhdGEpIHtcbiAgICAgICAgdmFyIGVsLCBleGlzdGluZ05vZGVzLCBpc1BhcmVudCwgaywgbCwgbGVuLCBsZW4xLCBtYXhILCBtYXhXLCBwcmVkLCByLCByZWYsIHNnLCB0b0l0ZXJhdGU7XG4gICAgICAgIGV4aXN0aW5nTm9kZXMgPSBbXTtcbiAgICAgICAgaWYgKGRhdGEubm9kZXMgIT0gbnVsbCkge1xuICAgICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEubm9kZXM7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5zdGVwX2Z1bmN0aW9uO1xuICAgICAgICAgIGlzUGFyZW50ID0gdHJ1ZTtcbiAgICAgICAgfVxuICAgICAgICBmb3IgKGsgPSAwLCBsZW4gPSB0b0l0ZXJhdGUubGVuZ3RoOyBrIDwgbGVuOyBrKyspIHtcbiAgICAgICAgICBlbCA9IHRvSXRlcmF0ZVtrXTtcbiAgICAgICAgICBtYXhXID0gMDtcbiAgICAgICAgICBtYXhIID0gMDtcbiAgICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgICAgc2cgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7XG4gICAgICAgICAgICAgIG11bHRpZ3JhcGg6IHRydWUsXG4gICAgICAgICAgICAgIGNvbXBvdW5kOiB0cnVlXG4gICAgICAgICAgICB9KS5zZXRHcmFwaCh7XG4gICAgICAgICAgICAgIG5vZGVzZXA6IDIwLFxuICAgICAgICAgICAgICBlZGdlc2VwOiAwLFxuICAgICAgICAgICAgICByYW5rc2VwOiAyMCxcbiAgICAgICAgICAgICAgcmFua2RpcjogXCJMUlwiLFxuICAgICAgICAgICAgICBtYXJnaW54OiAxMCxcbiAgICAgICAgICAgICAgbWFyZ2lueTogMTBcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgc3ViZ3JhcGhzW2VsLmlkXSA9IHNnO1xuICAgICAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKHNnLCBlbCk7XG4gICAgICAgICAgICByID0gbmV3IGRhZ3JlRDMucmVuZGVyKCk7XG4gICAgICAgICAgICBkM3RtcFN2Zy5zZWxlY3QoJ2cnKS5jYWxsKHIsIHNnKTtcbiAgICAgICAgICAgIG1heFcgPSBzZy5ncmFwaCgpLndpZHRoO1xuICAgICAgICAgICAgbWF4SCA9IHNnLmdyYXBoKCkuaGVpZ2h0O1xuICAgICAgICAgICAgYW5ndWxhci5lbGVtZW50KG1haW5UbXBFbGVtZW50KS5lbXB0eSgpO1xuICAgICAgICAgIH1cbiAgICAgICAgICBjcmVhdGVOb2RlKGcsIGRhdGEsIGVsLCBpc1BhcmVudCwgbWF4VywgbWF4SCk7XG4gICAgICAgICAgZXhpc3RpbmdOb2Rlcy5wdXNoKGVsLmlkKTtcbiAgICAgICAgICBpZiAoZWwuaW5wdXRzICE9IG51bGwpIHtcbiAgICAgICAgICAgIHJlZiA9IGVsLmlucHV0cztcbiAgICAgICAgICAgIGZvciAobCA9IDAsIGxlbjEgPSByZWYubGVuZ3RoOyBsIDwgbGVuMTsgbCsrKSB7XG4gICAgICAgICAgICAgIHByZWQgPSByZWZbbF07XG4gICAgICAgICAgICAgIGNyZWF0ZUVkZ2UoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gZztcbiAgICAgIH07XG4gICAgICBzZWFyY2hGb3JOb2RlID0gZnVuY3Rpb24oZGF0YSwgbm9kZUlEKSB7XG4gICAgICAgIHZhciBlbCwgaSwgajtcbiAgICAgICAgZm9yIChpIGluIGRhdGEubm9kZXMpIHtcbiAgICAgICAgICBlbCA9IGRhdGEubm9kZXNbaV07XG4gICAgICAgICAgaWYgKGVsLmlkID09PSBub2RlSUQpIHtcbiAgICAgICAgICAgIHJldHVybiBlbDtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24gIT0gbnVsbCkge1xuICAgICAgICAgICAgZm9yIChqIGluIGVsLnN0ZXBfZnVuY3Rpb24pIHtcbiAgICAgICAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb25bal0uaWQgPT09IG5vZGVJRCkge1xuICAgICAgICAgICAgICAgIHJldHVybiBlbC5zdGVwX2Z1bmN0aW9uW2pdO1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgZHJhd0dyYXBoID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgaSwgbmV3U2NhbGUsIHJlbmRlcmVyLCBzZywgeENlbnRlck9mZnNldCwgeUNlbnRlck9mZnNldDtcbiAgICAgICAgZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHtcbiAgICAgICAgICBtdWx0aWdyYXBoOiB0cnVlLFxuICAgICAgICAgIGNvbXBvdW5kOiB0cnVlXG4gICAgICAgIH0pLnNldEdyYXBoKHtcbiAgICAgICAgICBub2Rlc2VwOiA3MCxcbiAgICAgICAgICBlZGdlc2VwOiAwLFxuICAgICAgICAgIHJhbmtzZXA6IDUwLFxuICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIixcbiAgICAgICAgICBtYXJnaW54OiA0MCxcbiAgICAgICAgICBtYXJnaW55OiA0MFxuICAgICAgICB9KTtcbiAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKGcsIGRhdGEpO1xuICAgICAgICByZW5kZXJlciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpO1xuICAgICAgICBkM21haW5TdmdHLmNhbGwocmVuZGVyZXIsIGcpO1xuICAgICAgICBmb3IgKGkgaW4gc3ViZ3JhcGhzKSB7XG4gICAgICAgICAgc2cgPSBzdWJncmFwaHNbaV07XG4gICAgICAgICAgZDNtYWluU3ZnLnNlbGVjdCgnc3ZnLnN2Zy0nICsgaSArICcgZycpLmNhbGwocmVuZGVyZXIsIHNnKTtcbiAgICAgICAgfVxuICAgICAgICBuZXdTY2FsZSA9IDAuNTtcbiAgICAgICAgeENlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkud2lkdGgoKSAtIGcuZ3JhcGgoKS53aWR0aCAqIG5ld1NjYWxlKSAvIDIpO1xuICAgICAgICB5Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS5oZWlnaHQoKSAtIGcuZ3JhcGgoKS5oZWlnaHQgKiBuZXdTY2FsZSkgLyAyKTtcbiAgICAgICAgbWFpblpvb20uc2NhbGUobmV3U2NhbGUpLnRyYW5zbGF0ZShbeENlbnRlck9mZnNldCwgeUNlbnRlck9mZnNldF0pO1xuICAgICAgICBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB4Q2VudGVyT2Zmc2V0ICsgXCIsIFwiICsgeUNlbnRlck9mZnNldCArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgIG1haW5ab29tLm9uKFwiem9vbVwiLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICB2YXIgZXY7XG4gICAgICAgICAgZXYgPSBkMy5ldmVudDtcbiAgICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgZXYudHJhbnNsYXRlICsgXCIpIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIilcIik7XG4gICAgICAgIH0pO1xuICAgICAgICBtYWluWm9vbShkM21haW5TdmcpO1xuICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5zZWxlY3RBbGwoJy5ub2RlJykub24oJ2NsaWNrJywgZnVuY3Rpb24oZCkge1xuICAgICAgICAgIHJldHVybiBzY29wZS5zZXROb2RlKHtcbiAgICAgICAgICAgIG5vZGVpZDogZFxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgICBzY29wZS4kd2F0Y2goYXR0cnMucGxhbiwgZnVuY3Rpb24obmV3UGxhbikge1xuICAgICAgICBpZiAobmV3UGxhbikge1xuICAgICAgICAgIHJldHVybiBkcmF3R3JhcGgobmV3UGxhbik7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4uc2VydmljZSAnSm9ic1NlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkbG9nLCBhbU1vbWVudCwgJHEsICR0aW1lb3V0KSAtPlxyXG4gIGN1cnJlbnRKb2IgPSBudWxsXHJcbiAgY3VycmVudFBsYW4gPSBudWxsXHJcblxyXG4gIGRlZmVycmVkcyA9IHt9XHJcbiAgam9icyA9IHtcclxuICAgIHJ1bm5pbmc6IFtdXHJcbiAgICBmaW5pc2hlZDogW11cclxuICAgIGNhbmNlbGxlZDogW11cclxuICAgIGZhaWxlZDogW11cclxuICB9XHJcblxyXG4gIGpvYk9ic2VydmVycyA9IFtdXHJcblxyXG4gIG5vdGlmeU9ic2VydmVycyA9IC0+XHJcbiAgICBhbmd1bGFyLmZvckVhY2ggam9iT2JzZXJ2ZXJzLCAoY2FsbGJhY2spIC0+XHJcbiAgICAgIGNhbGxiYWNrKClcclxuXHJcbiAgQHJlZ2lzdGVyT2JzZXJ2ZXIgPSAoY2FsbGJhY2spIC0+XHJcbiAgICBqb2JPYnNlcnZlcnMucHVzaChjYWxsYmFjaylcclxuXHJcbiAgQHVuUmVnaXN0ZXJPYnNlcnZlciA9IChjYWxsYmFjaykgLT5cclxuICAgIGluZGV4ID0gam9iT2JzZXJ2ZXJzLmluZGV4T2YoY2FsbGJhY2spXHJcbiAgICBqb2JPYnNlcnZlcnMuc3BsaWNlKGluZGV4LCAxKVxyXG5cclxuICBAc3RhdGVMaXN0ID0gLT5cclxuICAgIFsgXHJcbiAgICAgICMgJ0NSRUFURUQnXHJcbiAgICAgICdTQ0hFRFVMRUQnXHJcbiAgICAgICdERVBMT1lJTkcnXHJcbiAgICAgICdSVU5OSU5HJ1xyXG4gICAgICAnRklOSVNIRUQnXHJcbiAgICAgICdGQUlMRUQnXHJcbiAgICAgICdDQU5DRUxJTkcnXHJcbiAgICAgICdDQU5DRUxFRCdcclxuICAgIF1cclxuXHJcbiAgQHRyYW5zbGF0ZUxhYmVsU3RhdGUgPSAoc3RhdGUpIC0+XHJcbiAgICBzd2l0Y2ggc3RhdGUudG9Mb3dlckNhc2UoKVxyXG4gICAgICB3aGVuICdmaW5pc2hlZCcgdGhlbiAnc3VjY2VzcydcclxuICAgICAgd2hlbiAnZmFpbGVkJyB0aGVuICdkYW5nZXInXHJcbiAgICAgIHdoZW4gJ3NjaGVkdWxlZCcgdGhlbiAnZGVmYXVsdCdcclxuICAgICAgd2hlbiAnZGVwbG95aW5nJyB0aGVuICdpbmZvJ1xyXG4gICAgICB3aGVuICdydW5uaW5nJyB0aGVuICdwcmltYXJ5J1xyXG4gICAgICB3aGVuICdjYW5jZWxpbmcnIHRoZW4gJ3dhcm5pbmcnXHJcbiAgICAgIHdoZW4gJ3BlbmRpbmcnIHRoZW4gJ2luZm8nXHJcbiAgICAgIHdoZW4gJ3RvdGFsJyB0aGVuICdibGFjaydcclxuICAgICAgZWxzZSAnZGVmYXVsdCdcclxuXHJcbiAgQHNldEVuZFRpbWVzID0gKGxpc3QpIC0+XHJcbiAgICBhbmd1bGFyLmZvckVhY2ggbGlzdCwgKGl0ZW0sIGpvYktleSkgLT5cclxuICAgICAgdW5sZXNzIGl0ZW1bJ2VuZC10aW1lJ10gPiAtMVxyXG4gICAgICAgIGl0ZW1bJ2VuZC10aW1lJ10gPSBpdGVtWydzdGFydC10aW1lJ10gKyBpdGVtWydkdXJhdGlvbiddXHJcblxyXG4gIEBwcm9jZXNzVmVydGljZXMgPSAoZGF0YSkgLT5cclxuICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLnZlcnRpY2VzLCAodmVydGV4LCBpKSAtPlxyXG4gICAgICB2ZXJ0ZXgudHlwZSA9ICdyZWd1bGFyJ1xyXG5cclxuICAgIGRhdGEudmVydGljZXMudW5zaGlmdCh7XHJcbiAgICAgIG5hbWU6ICdTY2hlZHVsZWQnXHJcbiAgICAgICdzdGFydC10aW1lJzogZGF0YS50aW1lc3RhbXBzWydDUkVBVEVEJ11cclxuICAgICAgJ2VuZC10aW1lJzogZGF0YS50aW1lc3RhbXBzWydDUkVBVEVEJ10gKyAxXHJcbiAgICAgIHR5cGU6ICdzY2hlZHVsZWQnXHJcbiAgICB9KVxyXG5cclxuICBAbGlzdEpvYnMgPSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9ib3ZlcnZpZXdcIlxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSA9PlxyXG4gICAgICBhbmd1bGFyLmZvckVhY2ggZGF0YSwgKGxpc3QsIGxpc3RLZXkpID0+XHJcbiAgICAgICAgc3dpdGNoIGxpc3RLZXlcclxuICAgICAgICAgIHdoZW4gJ3J1bm5pbmcnIHRoZW4gam9icy5ydW5uaW5nID0gQHNldEVuZFRpbWVzKGxpc3QpXHJcbiAgICAgICAgICB3aGVuICdmaW5pc2hlZCcgdGhlbiBqb2JzLmZpbmlzaGVkID0gQHNldEVuZFRpbWVzKGxpc3QpXHJcbiAgICAgICAgICB3aGVuICdjYW5jZWxsZWQnIHRoZW4gam9icy5jYW5jZWxsZWQgPSBAc2V0RW5kVGltZXMobGlzdClcclxuICAgICAgICAgIHdoZW4gJ2ZhaWxlZCcgdGhlbiBqb2JzLmZhaWxlZCA9IEBzZXRFbmRUaW1lcyhsaXN0KVxyXG5cclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShqb2JzKVxyXG4gICAgICBub3RpZnlPYnNlcnZlcnMoKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGdldEpvYnMgPSAodHlwZSkgLT5cclxuICAgIGpvYnNbdHlwZV1cclxuXHJcbiAgQGdldEFsbEpvYnMgPSAtPlxyXG4gICAgam9ic1xyXG5cclxuICBAbG9hZEpvYiA9IChqb2JpZCkgLT5cclxuICAgIGN1cnJlbnRKb2IgPSBudWxsXHJcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZFxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSA9PlxyXG4gICAgICBAc2V0RW5kVGltZXMoZGF0YS52ZXJ0aWNlcylcclxuICAgICAgQHByb2Nlc3NWZXJ0aWNlcyhkYXRhKVxyXG5cclxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIvY29uZmlnXCJcclxuICAgICAgLnN1Y2Nlc3MgKGpvYkNvbmZpZykgLT5cclxuICAgICAgICBkYXRhID0gYW5ndWxhci5leHRlbmQoZGF0YSwgam9iQ29uZmlnKVxyXG5cclxuICAgICAgICBjdXJyZW50Sm9iID0gZGF0YVxyXG5cclxuICAgICAgICBkZWZlcnJlZHMuam9iLnJlc29sdmUoY3VycmVudEpvYilcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2VcclxuXHJcbiAgQGdldE5vZGUgPSAobm9kZWlkKSAtPlxyXG4gICAgc2Vla05vZGUgPSAobm9kZWlkLCBkYXRhKSAtPlxyXG4gICAgICBmb3Igbm9kZSBpbiBkYXRhXHJcbiAgICAgICAgcmV0dXJuIG5vZGUgaWYgbm9kZS5pZCBpcyBub2RlaWRcclxuICAgICAgICBzdWIgPSBzZWVrTm9kZShub2RlaWQsIG5vZGUuc3RlcF9mdW5jdGlvbikgaWYgbm9kZS5zdGVwX2Z1bmN0aW9uXHJcbiAgICAgICAgcmV0dXJuIHN1YiBpZiBzdWJcclxuXHJcbiAgICAgIG51bGxcclxuXHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cclxuICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50Sm9iLnBsYW4ubm9kZXMpXHJcblxyXG4gICAgICBmb3VuZE5vZGUudmVydGV4ID0gQHNlZWtWZXJ0ZXgobm9kZWlkKVxyXG5cclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShmb3VuZE5vZGUpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAc2Vla1ZlcnRleCA9IChub2RlaWQpIC0+XHJcbiAgICBmb3IgdmVydGV4IGluIGN1cnJlbnRKb2IudmVydGljZXNcclxuICAgICAgcmV0dXJuIHZlcnRleCBpZiB2ZXJ0ZXguaWQgaXMgbm9kZWlkXHJcblxyXG4gICAgcmV0dXJuIG51bGxcclxuXHJcbiAgQGdldFZlcnRleCA9ICh2ZXJ0ZXhpZCkgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxyXG4gICAgICB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcclxuXHJcbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrdGltZXNcIlxyXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgPT5cclxuICAgICAgICAjIFRPRE86IGNoYW5nZSB0byBzdWJ0YXNrdGltZXNcclxuICAgICAgICB2ZXJ0ZXguc3VidGFza3MgPSBkYXRhLnN1YnRhc2tzXHJcblxyXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUodmVydGV4KVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGdldFN1YnRhc2tzID0gKHZlcnRleGlkKSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XHJcbiAgICAgICMgdmVydGV4ID0gQHNlZWtWZXJ0ZXgodmVydGV4aWQpXHJcblxyXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkXHJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxyXG4gICAgICAgIHN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrc1xyXG5cclxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHN1YnRhc2tzKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGdldFRhc2tNYW5hZ2VycyA9ICh2ZXJ0ZXhpZCkgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxyXG4gICAgICAjIHZlcnRleCA9IEBzZWVrVmVydGV4KHZlcnRleGlkKVxyXG5cclxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3Rhc2ttYW5hZ2Vyc1wiXHJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxyXG4gICAgICAgIHRhc2ttYW5hZ2VycyA9IGRhdGEudGFza21hbmFnZXJzXHJcblxyXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUodGFza21hbmFnZXJzKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGdldEFjY3VtdWxhdG9ycyA9ICh2ZXJ0ZXhpZCkgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxyXG4gICAgICAjIHZlcnRleCA9IEBzZWVrVmVydGV4KHZlcnRleGlkKVxyXG4gICAgICBjb25zb2xlLmxvZyhjdXJyZW50Sm9iLmppZClcclxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL2FjY3VtdWxhdG9yc1wiXHJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxyXG4gICAgICAgIGFjY3VtdWxhdG9ycyA9IGRhdGFbJ3VzZXItYWNjdW11bGF0b3JzJ11cclxuXHJcbiAgICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3N1YnRhc2tzL2FjY3VtdWxhdG9yc1wiXHJcbiAgICAgICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XHJcbiAgICAgICAgICBzdWJ0YXNrQWNjdW11bGF0b3JzID0gZGF0YS5zdWJ0YXNrc1xyXG5cclxuICAgICAgICAgIGRlZmVycmVkLnJlc29sdmUoeyBtYWluOiBhY2N1bXVsYXRvcnMsIHN1YnRhc2tzOiBzdWJ0YXNrQWNjdW11bGF0b3JzIH0pXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICAjIENoZWNrcG9pbnQgY29uZmlnXHJcbiAgQGdldENoZWNrcG9pbnRDb25maWcgPSAgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxyXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi9jaGVja3BvaW50cy9jb25maWdcIlxyXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cclxuICAgICAgICBpZiAoYW5ndWxhci5lcXVhbHMoe30sIGRhdGEpKVxyXG4gICAgICAgICAgZGVmZXJyZWQucmVzb2x2ZShudWxsKVxyXG4gICAgICAgIGVsc2VcclxuICAgICAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gICMgR2VuZXJhbCBjaGVja3BvaW50IHN0YXRzIGxpa2UgY291bnRzLCBoaXN0b3J5LCBldGMuXHJcbiAgQGdldENoZWNrcG9pbnRTdGF0cyA9IC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cclxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvY2hlY2twb2ludHNcIlxyXG4gICAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpID0+XHJcbiAgICAgICAgaWYgKGFuZ3VsYXIuZXF1YWxzKHt9LCBkYXRhKSlcclxuICAgICAgICAgIGRlZmVycmVkLnJlc29sdmUobnVsbClcclxuICAgICAgICBlbHNlXHJcbiAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICAjIERldGFpbGVkIGNoZWNrcG9pbnQgc3RhdHMgZm9yIGEgc2luZ2xlIGNoZWNrcG9pbnRcclxuICBAZ2V0Q2hlY2twb2ludERldGFpbHMgPSAoY2hlY2twb2ludGlkKSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XHJcbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2NoZWNrcG9pbnRzL2RldGFpbHMvXCIgKyBjaGVja3BvaW50aWRcclxuICAgICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XHJcbiAgICAgICAgIyBJZiBubyBkYXRhIGF2YWlsYWJsZSwgd2UgYXJlIGRvbmUuXHJcbiAgICAgICAgaWYgKGFuZ3VsYXIuZXF1YWxzKHt9LCBkYXRhKSlcclxuICAgICAgICAgIGRlZmVycmVkLnJlc29sdmUobnVsbClcclxuICAgICAgICBlbHNlXHJcbiAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICAjIERldGFpbGVkIHN1YnRhc2sgc3RhdHMgZm9yIGEgc2luZ2xlIGNoZWNrcG9pbnRcclxuICBAZ2V0Q2hlY2twb2ludFN1YnRhc2tEZXRhaWxzID0gKGNoZWNrcG9pbnRpZCwgdmVydGV4aWQpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cclxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvY2hlY2twb2ludHMvZGV0YWlscy9cIiArIGNoZWNrcG9pbnRpZCArIFwiL3N1YnRhc2tzL1wiICsgdmVydGV4aWRcclxuICAgICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XHJcbiAgICAgICAgIyBJZiBubyBkYXRhIGF2YWlsYWJsZSwgd2UgYXJlIGRvbmUuXHJcbiAgICAgICAgaWYgKGFuZ3VsYXIuZXF1YWxzKHt9LCBkYXRhKSlcclxuICAgICAgICAgIGRlZmVycmVkLnJlc29sdmUobnVsbClcclxuICAgICAgICBlbHNlXHJcbiAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICAjIE9wZXJhdG9yLWxldmVsIGJhY2sgcHJlc3N1cmUgc3RhdHNcclxuICBAZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmUgPSAodmVydGV4aWQpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvYmFja3ByZXNzdXJlXCJcclxuICAgIC5zdWNjZXNzIChkYXRhKSA9PlxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAdHJhbnNsYXRlQmFja1ByZXNzdXJlTGFiZWxTdGF0ZSA9IChzdGF0ZSkgLT5cclxuICAgIHN3aXRjaCBzdGF0ZS50b0xvd2VyQ2FzZSgpXHJcbiAgICAgIHdoZW4gJ2luLXByb2dyZXNzJyB0aGVuICdkYW5nZXInXHJcbiAgICAgIHdoZW4gJ29rJyB0aGVuICdzdWNjZXNzJ1xyXG4gICAgICB3aGVuICdsb3cnIHRoZW4gJ3dhcm5pbmcnXHJcbiAgICAgIHdoZW4gJ2hpZ2gnIHRoZW4gJ2RhbmdlcidcclxuICAgICAgZWxzZSAnZGVmYXVsdCdcclxuXHJcbiAgQGxvYWRFeGNlcHRpb25zID0gLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxyXG5cclxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvZXhjZXB0aW9uc1wiXHJcbiAgICAgIC5zdWNjZXNzIChleGNlcHRpb25zKSAtPlxyXG4gICAgICAgIGN1cnJlbnRKb2IuZXhjZXB0aW9ucyA9IGV4Y2VwdGlvbnNcclxuXHJcbiAgICAgICAgZGVmZXJyZWQucmVzb2x2ZShleGNlcHRpb25zKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGNhbmNlbEpvYiA9IChqb2JpZCkgLT5cclxuICAgICMgdXNlcyB0aGUgbm9uIFJFU1QtY29tcGxpYW50IEdFVCB5YXJuLWNhbmNlbCBoYW5kbGVyIHdoaWNoIGlzIGF2YWlsYWJsZSBpbiBhZGRpdGlvbiB0byB0aGVcclxuICAgICMgcHJvcGVyIFwiREVMRVRFIGpvYnMvPGpvYmlkPi9cIlxyXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIveWFybi1jYW5jZWxcIlxyXG5cclxuICBAc3RvcEpvYiA9IChqb2JpZCkgLT5cclxuICAgICMgdXNlcyB0aGUgbm9uIFJFU1QtY29tcGxpYW50IEdFVCB5YXJuLWNhbmNlbCBoYW5kbGVyIHdoaWNoIGlzIGF2YWlsYWJsZSBpbiBhZGRpdGlvbiB0byB0aGVcclxuICAgICMgcHJvcGVyIFwiREVMRVRFIGpvYnMvPGpvYmlkPi9cIlxyXG4gICAgJGh0dHAuZ2V0IFwiam9icy9cIiArIGpvYmlkICsgXCIveWFybi1zdG9wXCJcclxuXHJcbiAgQFxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdKb2JzU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJGxvZywgYW1Nb21lbnQsICRxLCAkdGltZW91dCkge1xuICB2YXIgY3VycmVudEpvYiwgY3VycmVudFBsYW4sIGRlZmVycmVkcywgam9iT2JzZXJ2ZXJzLCBqb2JzLCBub3RpZnlPYnNlcnZlcnM7XG4gIGN1cnJlbnRKb2IgPSBudWxsO1xuICBjdXJyZW50UGxhbiA9IG51bGw7XG4gIGRlZmVycmVkcyA9IHt9O1xuICBqb2JzID0ge1xuICAgIHJ1bm5pbmc6IFtdLFxuICAgIGZpbmlzaGVkOiBbXSxcbiAgICBjYW5jZWxsZWQ6IFtdLFxuICAgIGZhaWxlZDogW11cbiAgfTtcbiAgam9iT2JzZXJ2ZXJzID0gW107XG4gIG5vdGlmeU9ic2VydmVycyA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2goam9iT2JzZXJ2ZXJzLCBmdW5jdGlvbihjYWxsYmFjaykge1xuICAgICAgcmV0dXJuIGNhbGxiYWNrKCk7XG4gICAgfSk7XG4gIH07XG4gIHRoaXMucmVnaXN0ZXJPYnNlcnZlciA9IGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgcmV0dXJuIGpvYk9ic2VydmVycy5wdXNoKGNhbGxiYWNrKTtcbiAgfTtcbiAgdGhpcy51blJlZ2lzdGVyT2JzZXJ2ZXIgPSBmdW5jdGlvbihjYWxsYmFjaykge1xuICAgIHZhciBpbmRleDtcbiAgICBpbmRleCA9IGpvYk9ic2VydmVycy5pbmRleE9mKGNhbGxiYWNrKTtcbiAgICByZXR1cm4gam9iT2JzZXJ2ZXJzLnNwbGljZShpbmRleCwgMSk7XG4gIH07XG4gIHRoaXMuc3RhdGVMaXN0ID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIFsnU0NIRURVTEVEJywgJ0RFUExPWUlORycsICdSVU5OSU5HJywgJ0ZJTklTSEVEJywgJ0ZBSUxFRCcsICdDQU5DRUxJTkcnLCAnQ0FOQ0VMRUQnXTtcbiAgfTtcbiAgdGhpcy50cmFuc2xhdGVMYWJlbFN0YXRlID0gZnVuY3Rpb24oc3RhdGUpIHtcbiAgICBzd2l0Y2ggKHN0YXRlLnRvTG93ZXJDYXNlKCkpIHtcbiAgICAgIGNhc2UgJ2ZpbmlzaGVkJzpcbiAgICAgICAgcmV0dXJuICdzdWNjZXNzJztcbiAgICAgIGNhc2UgJ2ZhaWxlZCc6XG4gICAgICAgIHJldHVybiAnZGFuZ2VyJztcbiAgICAgIGNhc2UgJ3NjaGVkdWxlZCc6XG4gICAgICAgIHJldHVybiAnZGVmYXVsdCc7XG4gICAgICBjYXNlICdkZXBsb3lpbmcnOlxuICAgICAgICByZXR1cm4gJ2luZm8nO1xuICAgICAgY2FzZSAncnVubmluZyc6XG4gICAgICAgIHJldHVybiAncHJpbWFyeSc7XG4gICAgICBjYXNlICdjYW5jZWxpbmcnOlxuICAgICAgICByZXR1cm4gJ3dhcm5pbmcnO1xuICAgICAgY2FzZSAncGVuZGluZyc6XG4gICAgICAgIHJldHVybiAnaW5mbyc7XG4gICAgICBjYXNlICd0b3RhbCc6XG4gICAgICAgIHJldHVybiAnYmxhY2snO1xuICAgICAgZGVmYXVsdDpcbiAgICAgICAgcmV0dXJuICdkZWZhdWx0JztcbiAgICB9XG4gIH07XG4gIHRoaXMuc2V0RW5kVGltZXMgPSBmdW5jdGlvbihsaXN0KSB7XG4gICAgcmV0dXJuIGFuZ3VsYXIuZm9yRWFjaChsaXN0LCBmdW5jdGlvbihpdGVtLCBqb2JLZXkpIHtcbiAgICAgIGlmICghKGl0ZW1bJ2VuZC10aW1lJ10gPiAtMSkpIHtcbiAgICAgICAgcmV0dXJuIGl0ZW1bJ2VuZC10aW1lJ10gPSBpdGVtWydzdGFydC10aW1lJ10gKyBpdGVtWydkdXJhdGlvbiddO1xuICAgICAgfVxuICAgIH0pO1xuICB9O1xuICB0aGlzLnByb2Nlc3NWZXJ0aWNlcyA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICBhbmd1bGFyLmZvckVhY2goZGF0YS52ZXJ0aWNlcywgZnVuY3Rpb24odmVydGV4LCBpKSB7XG4gICAgICByZXR1cm4gdmVydGV4LnR5cGUgPSAncmVndWxhcic7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRhdGEudmVydGljZXMudW5zaGlmdCh7XG4gICAgICBuYW1lOiAnU2NoZWR1bGVkJyxcbiAgICAgICdzdGFydC10aW1lJzogZGF0YS50aW1lc3RhbXBzWydDUkVBVEVEJ10sXG4gICAgICAnZW5kLXRpbWUnOiBkYXRhLnRpbWVzdGFtcHNbJ0NSRUFURUQnXSArIDEsXG4gICAgICB0eXBlOiAnc2NoZWR1bGVkJ1xuICAgIH0pO1xuICB9O1xuICB0aGlzLmxpc3RKb2JzID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JvdmVydmlld1wiKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLCBmdW5jdGlvbihsaXN0LCBsaXN0S2V5KSB7XG4gICAgICAgICAgc3dpdGNoIChsaXN0S2V5KSB7XG4gICAgICAgICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMucnVubmluZyA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgICAgY2FzZSAnZmluaXNoZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5maW5pc2hlZCA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgICAgY2FzZSAnY2FuY2VsbGVkJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMuY2FuY2VsbGVkID0gX3RoaXMuc2V0RW5kVGltZXMobGlzdCk7XG4gICAgICAgICAgICBjYXNlICdmYWlsZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5mYWlsZWQgPSBfdGhpcy5zZXRFbmRUaW1lcyhsaXN0KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpO1xuICAgICAgICByZXR1cm4gbm90aWZ5T2JzZXJ2ZXJzKCk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRKb2JzID0gZnVuY3Rpb24odHlwZSkge1xuICAgIHJldHVybiBqb2JzW3R5cGVdO1xuICB9O1xuICB0aGlzLmdldEFsbEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gam9icztcbiAgfTtcbiAgdGhpcy5sb2FkSm9iID0gZnVuY3Rpb24oam9iaWQpIHtcbiAgICBjdXJyZW50Sm9iID0gbnVsbDtcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQpLnN1Y2Nlc3MoKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgICAgX3RoaXMuc2V0RW5kVGltZXMoZGF0YS52ZXJ0aWNlcyk7XG4gICAgICAgIF90aGlzLnByb2Nlc3NWZXJ0aWNlcyhkYXRhKTtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL2NvbmZpZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGpvYkNvbmZpZykge1xuICAgICAgICAgIGRhdGEgPSBhbmd1bGFyLmV4dGVuZChkYXRhLCBqb2JDb25maWcpO1xuICAgICAgICAgIGN1cnJlbnRKb2IgPSBkYXRhO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZHMuam9iLnJlc29sdmUoY3VycmVudEpvYik7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkcy5qb2IucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXROb2RlID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgdmFyIGRlZmVycmVkLCBzZWVrTm9kZTtcbiAgICBzZWVrTm9kZSA9IGZ1bmN0aW9uKG5vZGVpZCwgZGF0YSkge1xuICAgICAgdmFyIGosIGxlbiwgbm9kZSwgc3ViO1xuICAgICAgZm9yIChqID0gMCwgbGVuID0gZGF0YS5sZW5ndGg7IGogPCBsZW47IGorKykge1xuICAgICAgICBub2RlID0gZGF0YVtqXTtcbiAgICAgICAgaWYgKG5vZGUuaWQgPT09IG5vZGVpZCkge1xuICAgICAgICAgIHJldHVybiBub2RlO1xuICAgICAgICB9XG4gICAgICAgIGlmIChub2RlLnN0ZXBfZnVuY3Rpb24pIHtcbiAgICAgICAgICBzdWIgPSBzZWVrTm9kZShub2RlaWQsIG5vZGUuc3RlcF9mdW5jdGlvbik7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKHN1Yikge1xuICAgICAgICAgIHJldHVybiBzdWI7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHJldHVybiBudWxsO1xuICAgIH07XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGZvdW5kTm9kZTtcbiAgICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50Sm9iLnBsYW4ubm9kZXMpO1xuICAgICAgICBmb3VuZE5vZGUudmVydGV4ID0gX3RoaXMuc2Vla1ZlcnRleChub2RlaWQpO1xuICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShmb3VuZE5vZGUpO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuc2Vla1ZlcnRleCA9IGZ1bmN0aW9uKG5vZGVpZCkge1xuICAgIHZhciBqLCBsZW4sIHJlZiwgdmVydGV4O1xuICAgIHJlZiA9IGN1cnJlbnRKb2IudmVydGljZXM7XG4gICAgZm9yIChqID0gMCwgbGVuID0gcmVmLmxlbmd0aDsgaiA8IGxlbjsgaisrKSB7XG4gICAgICB2ZXJ0ZXggPSByZWZbal07XG4gICAgICBpZiAodmVydGV4LmlkID09PSBub2RlaWQpIHtcbiAgICAgICAgcmV0dXJuIHZlcnRleDtcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH07XG4gIHRoaXMuZ2V0VmVydGV4ID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIHZlcnRleDtcbiAgICAgICAgdmVydGV4ID0gX3RoaXMuc2Vla1ZlcnRleCh2ZXJ0ZXhpZCk7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvc3VidGFza3RpbWVzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZlcnRleC5zdWJ0YXNrcyA9IGRhdGEuc3VidGFza3M7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUodmVydGV4KTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRTdWJ0YXNrcyA9IGZ1bmN0aW9uKHZlcnRleGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICB2YXIgc3VidGFza3M7XG4gICAgICAgICAgc3VidGFza3MgPSBkYXRhLnN1YnRhc2tzO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKHN1YnRhc2tzKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRUYXNrTWFuYWdlcnMgPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3Rhc2ttYW5hZ2Vyc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICB2YXIgdGFza21hbmFnZXJzO1xuICAgICAgICAgIHRhc2ttYW5hZ2VycyA9IGRhdGEudGFza21hbmFnZXJzO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKHRhc2ttYW5hZ2Vycyk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0QWNjdW11bGF0b3JzID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgY29uc29sZS5sb2coY3VycmVudEpvYi5qaWQpO1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL2FjY3VtdWxhdG9yc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICB2YXIgYWNjdW11bGF0b3JzO1xuICAgICAgICAgIGFjY3VtdWxhdG9ycyA9IGRhdGFbJ3VzZXItYWNjdW11bGF0b3JzJ107XG4gICAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrcy9hY2N1bXVsYXRvcnNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgICB2YXIgc3VidGFza0FjY3VtdWxhdG9ycztcbiAgICAgICAgICAgIHN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzO1xuICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoe1xuICAgICAgICAgICAgICBtYWluOiBhY2N1bXVsYXRvcnMsXG4gICAgICAgICAgICAgIHN1YnRhc2tzOiBzdWJ0YXNrQWNjdW11bGF0b3JzXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRDaGVja3BvaW50Q29uZmlnID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi9jaGVja3BvaW50cy9jb25maWdcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgaWYgKGFuZ3VsYXIuZXF1YWxzKHt9LCBkYXRhKSkge1xuICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUobnVsbCk7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRDaGVja3BvaW50U3RhdHMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2NoZWNrcG9pbnRzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgICAgICBpZiAoYW5ndWxhci5lcXVhbHMoe30sIGRhdGEpKSB7XG4gICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShudWxsKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldENoZWNrcG9pbnREZXRhaWxzID0gZnVuY3Rpb24oY2hlY2twb2ludGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi9jaGVja3BvaW50cy9kZXRhaWxzL1wiICsgY2hlY2twb2ludGlkKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICBpZiAoYW5ndWxhci5lcXVhbHMoe30sIGRhdGEpKSB7XG4gICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShudWxsKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldENoZWNrcG9pbnRTdWJ0YXNrRGV0YWlscyA9IGZ1bmN0aW9uKGNoZWNrcG9pbnRpZCwgdmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2NoZWNrcG9pbnRzL2RldGFpbHMvXCIgKyBjaGVja3BvaW50aWQgKyBcIi9zdWJ0YXNrcy9cIiArIHZlcnRleGlkKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICBpZiAoYW5ndWxhci5lcXVhbHMoe30sIGRhdGEpKSB7XG4gICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShudWxsKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldE9wZXJhdG9yQmFja1ByZXNzdXJlID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9iYWNrcHJlc3N1cmVcIikuc3VjY2VzcygoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMudHJhbnNsYXRlQmFja1ByZXNzdXJlTGFiZWxTdGF0ZSA9IGZ1bmN0aW9uKHN0YXRlKSB7XG4gICAgc3dpdGNoIChzdGF0ZS50b0xvd2VyQ2FzZSgpKSB7XG4gICAgICBjYXNlICdpbi1wcm9ncmVzcyc6XG4gICAgICAgIHJldHVybiAnZGFuZ2VyJztcbiAgICAgIGNhc2UgJ29rJzpcbiAgICAgICAgcmV0dXJuICdzdWNjZXNzJztcbiAgICAgIGNhc2UgJ2xvdyc6XG4gICAgICAgIHJldHVybiAnd2FybmluZyc7XG4gICAgICBjYXNlICdoaWdoJzpcbiAgICAgICAgcmV0dXJuICdkYW5nZXInO1xuICAgICAgZGVmYXVsdDpcbiAgICAgICAgcmV0dXJuICdkZWZhdWx0JztcbiAgICB9XG4gIH07XG4gIHRoaXMubG9hZEV4Y2VwdGlvbnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2V4Y2VwdGlvbnNcIikuc3VjY2VzcyhmdW5jdGlvbihleGNlcHRpb25zKSB7XG4gICAgICAgICAgY3VycmVudEpvYi5leGNlcHRpb25zID0gZXhjZXB0aW9ucztcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShleGNlcHRpb25zKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5jYW5jZWxKb2IgPSBmdW5jdGlvbihqb2JpZCkge1xuICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi95YXJuLWNhbmNlbFwiKTtcbiAgfTtcbiAgdGhpcy5zdG9wSm9iID0gZnVuY3Rpb24oam9iaWQpIHtcbiAgICByZXR1cm4gJGh0dHAuZ2V0KFwiam9icy9cIiArIGpvYmlkICsgXCIveWFybi1zdG9wXCIpO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5kaXJlY3RpdmUgJ21ldHJpY3NHcmFwaCcsIC0+XHJcbiAgdGVtcGxhdGU6ICc8ZGl2IGNsYXNzPVwicGFuZWwgcGFuZWwtZGVmYXVsdCBwYW5lbC1tZXRyaWNcIj5cclxuICAgICAgICAgICAgICAgPGRpdiBjbGFzcz1cInBhbmVsLWhlYWRpbmdcIj5cclxuICAgICAgICAgICAgICAgICA8c3BhbiBjbGFzcz1cIm1ldHJpYy10aXRsZVwiPnt7bWV0cmljLmlkfX08L3NwYW4+XHJcbiAgICAgICAgICAgICAgICAgPGRpdiBjbGFzcz1cImJ1dHRvbnNcIj5cclxuICAgICAgICAgICAgICAgICAgIDxkaXYgY2xhc3M9XCJidG4tZ3JvdXBcIj5cclxuICAgICAgICAgICAgICAgICAgICAgPGJ1dHRvbiB0eXBlPVwiYnV0dG9uXCIgbmctY2xhc3M9XCJbYnRuQ2xhc3Nlcywge2FjdGl2ZTogbWV0cmljLnNpemUgIT0gXFwnYmlnXFwnfV1cIiBuZy1jbGljaz1cInNldFNpemUoXFwnc21hbGxcXCcpXCI+U21hbGw8L2J1dHRvbj5cclxuICAgICAgICAgICAgICAgICAgICAgPGJ1dHRvbiB0eXBlPVwiYnV0dG9uXCIgbmctY2xhc3M9XCJbYnRuQ2xhc3Nlcywge2FjdGl2ZTogbWV0cmljLnNpemUgPT0gXFwnYmlnXFwnfV1cIiBuZy1jbGljaz1cInNldFNpemUoXFwnYmlnXFwnKVwiPkJpZzwvYnV0dG9uPlxyXG4gICAgICAgICAgICAgICAgICAgPC9kaXY+XHJcbiAgICAgICAgICAgICAgICAgICA8YSB0aXRsZT1cIlJlbW92ZVwiIGNsYXNzPVwiYnRuIGJ0bi1kZWZhdWx0IGJ0bi14cyByZW1vdmVcIiBuZy1jbGljaz1cInJlbW92ZU1ldHJpYygpXCI+PGkgY2xhc3M9XCJmYSBmYS1jbG9zZVwiIC8+PC9hPlxyXG4gICAgICAgICAgICAgICAgIDwvZGl2PlxyXG4gICAgICAgICAgICAgICA8L2Rpdj5cclxuICAgICAgICAgICAgICAgPGRpdiBjbGFzcz1cInBhbmVsLWJvZHlcIj5cclxuICAgICAgICAgICAgICAgICAgPHN2ZyAvPlxyXG4gICAgICAgICAgICAgICA8L2Rpdj5cclxuICAgICAgICAgICAgIDwvZGl2PidcclxuICByZXBsYWNlOiB0cnVlXHJcbiAgc2NvcGU6XHJcbiAgICBtZXRyaWM6IFwiPVwiXHJcbiAgICB3aW5kb3c6IFwiPVwiXHJcbiAgICByZW1vdmVNZXRyaWM6IFwiJlwiXHJcbiAgICBzZXRNZXRyaWNTaXplOiBcIj1cIlxyXG4gICAgZ2V0VmFsdWVzOiBcIiZcIlxyXG5cclxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxyXG4gICAgc2NvcGUuYnRuQ2xhc3NlcyA9IFsnYnRuJywgJ2J0bi1kZWZhdWx0JywgJ2J0bi14cyddXHJcblxyXG4gICAgc2NvcGUudmFsdWUgPSBudWxsXHJcbiAgICBzY29wZS5kYXRhID0gW3tcclxuICAgICAgdmFsdWVzOiBzY29wZS5nZXRWYWx1ZXMoKVxyXG4gICAgfV1cclxuXHJcbiAgICBzY29wZS5vcHRpb25zID0ge1xyXG4gICAgICB4OiAoZCwgaSkgLT5cclxuICAgICAgICBkLnhcclxuICAgICAgeTogKGQsIGkpIC0+XHJcbiAgICAgICAgZC55XHJcblxyXG4gICAgICB4VGlja0Zvcm1hdDogKGQpIC0+XHJcbiAgICAgICAgZDMudGltZS5mb3JtYXQoJyVIOiVNOiVTJykobmV3IERhdGUoZCkpXHJcblxyXG4gICAgICB5VGlja0Zvcm1hdDogKGQpIC0+XHJcbiAgICAgICAgZm91bmQgPSBmYWxzZVxyXG4gICAgICAgIHBvdyA9IDBcclxuICAgICAgICBzdGVwID0gMVxyXG4gICAgICAgIGFic0QgPSBNYXRoLmFicyhkKVxyXG5cclxuICAgICAgICB3aGlsZSAhZm91bmQgJiYgcG93IDwgNTBcclxuICAgICAgICAgIGlmIE1hdGgucG93KDEwLCBwb3cpIDw9IGFic0QgJiYgYWJzRCA8IE1hdGgucG93KDEwLCBwb3cgKyBzdGVwKVxyXG4gICAgICAgICAgICBmb3VuZCA9IHRydWVcclxuICAgICAgICAgIGVsc2VcclxuICAgICAgICAgICAgcG93ICs9IHN0ZXBcclxuXHJcbiAgICAgICAgaWYgZm91bmQgJiYgcG93ID4gNlxyXG4gICAgICAgICAgXCIje2QgLyBNYXRoLnBvdygxMCwgcG93KX1FI3twb3d9XCJcclxuICAgICAgICBlbHNlXHJcbiAgICAgICAgICBcIiN7ZH1cIlxyXG4gICAgfVxyXG5cclxuICAgIHNjb3BlLnNob3dDaGFydCA9IC0+XHJcbiAgICAgIGQzLnNlbGVjdChlbGVtZW50LmZpbmQoXCJzdmdcIilbMF0pXHJcbiAgICAgIC5kYXR1bShzY29wZS5kYXRhKVxyXG4gICAgICAudHJhbnNpdGlvbigpLmR1cmF0aW9uKDI1MClcclxuICAgICAgLmNhbGwoc2NvcGUuY2hhcnQpXHJcblxyXG4gICAgc2NvcGUuY2hhcnQgPSBudi5tb2RlbHMubGluZUNoYXJ0KClcclxuICAgICAgLm9wdGlvbnMoc2NvcGUub3B0aW9ucylcclxuICAgICAgLnNob3dMZWdlbmQoZmFsc2UpXHJcbiAgICAgIC5tYXJnaW4oe1xyXG4gICAgICAgIHRvcDogMTVcclxuICAgICAgICBsZWZ0OiA2MFxyXG4gICAgICAgIGJvdHRvbTogMzBcclxuICAgICAgICByaWdodDogMzBcclxuICAgICAgfSlcclxuXHJcbiAgICBzY29wZS5jaGFydC55QXhpcy5zaG93TWF4TWluKGZhbHNlKVxyXG4gICAgc2NvcGUuY2hhcnQudG9vbHRpcC5oaWRlRGVsYXkoMClcclxuICAgIHNjb3BlLmNoYXJ0LnRvb2x0aXAuY29udGVudEdlbmVyYXRvcigob2JqKSAtPlxyXG4gICAgICBcIjxwPiN7ZDMudGltZS5mb3JtYXQoJyVIOiVNOiVTJykobmV3IERhdGUob2JqLnBvaW50LngpKX0gfCAje29iai5wb2ludC55fTwvcD5cIlxyXG4gICAgKVxyXG5cclxuICAgIG52LnV0aWxzLndpbmRvd1Jlc2l6ZShzY29wZS5jaGFydC51cGRhdGUpO1xyXG5cclxuICAgIHNjb3BlLnNldFNpemUgPSAoc2l6ZSkgLT5cclxuICAgICAgc2NvcGUuc2V0TWV0cmljU2l6ZShzY29wZS5tZXRyaWMsIHNpemUpXHJcblxyXG4gICAgc2NvcGUuc2hvd0NoYXJ0KClcclxuXHJcbiAgICBzY29wZS4kb24gJ21ldHJpY3M6ZGF0YTp1cGRhdGUnLCAoZXZlbnQsIHRpbWVzdGFtcCwgZGF0YSkgLT5cclxuIyAgICAgIHNjb3BlLnZhbHVlID0gcGFyc2VJbnQoZGF0YVtzY29wZS5tZXRyaWMuaWRdKVxyXG4gICAgICBzY29wZS52YWx1ZSA9IHBhcnNlRmxvYXQoZGF0YVtzY29wZS5tZXRyaWMuaWRdKVxyXG5cclxuICAgICAgc2NvcGUuZGF0YVswXS52YWx1ZXMucHVzaCB7XHJcbiAgICAgICAgeDogdGltZXN0YW1wXHJcbiAgICAgICAgeTogc2NvcGUudmFsdWVcclxuICAgICAgfVxyXG5cclxuICAgICAgaWYgc2NvcGUuZGF0YVswXS52YWx1ZXMubGVuZ3RoID4gc2NvcGUud2luZG93XHJcbiAgICAgICAgc2NvcGUuZGF0YVswXS52YWx1ZXMuc2hpZnQoKVxyXG5cclxuICAgICAgc2NvcGUuc2hvd0NoYXJ0KClcclxuICAgICAgc2NvcGUuY2hhcnQuY2xlYXJIaWdobGlnaHRzKClcclxuICAgICAgc2NvcGUuY2hhcnQudG9vbHRpcC5oaWRkZW4odHJ1ZSlcclxuXHJcbiAgICBlbGVtZW50LmZpbmQoXCIubWV0cmljLXRpdGxlXCIpLnF0aXAoe1xyXG4gICAgICBjb250ZW50OiB7XHJcbiAgICAgICAgdGV4dDogc2NvcGUubWV0cmljLmlkXHJcbiAgICAgIH0sXHJcbiAgICAgIHBvc2l0aW9uOiB7XHJcbiAgICAgICAgbXk6ICdib3R0b20gbGVmdCcsXHJcbiAgICAgICAgYXQ6ICd0b3AgbGVmdCdcclxuICAgICAgfSxcclxuICAgICAgc3R5bGU6IHtcclxuICAgICAgICBjbGFzc2VzOiAncXRpcC1saWdodCBxdGlwLXRpbWVsaW5lLWJhcidcclxuICAgICAgfVxyXG4gICAgfSk7XHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmRpcmVjdGl2ZSgnbWV0cmljc0dyYXBoJywgZnVuY3Rpb24oKSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6ICc8ZGl2IGNsYXNzPVwicGFuZWwgcGFuZWwtZGVmYXVsdCBwYW5lbC1tZXRyaWNcIj4gPGRpdiBjbGFzcz1cInBhbmVsLWhlYWRpbmdcIj4gPHNwYW4gY2xhc3M9XCJtZXRyaWMtdGl0bGVcIj57e21ldHJpYy5pZH19PC9zcGFuPiA8ZGl2IGNsYXNzPVwiYnV0dG9uc1wiPiA8ZGl2IGNsYXNzPVwiYnRuLWdyb3VwXCI+IDxidXR0b24gdHlwZT1cImJ1dHRvblwiIG5nLWNsYXNzPVwiW2J0bkNsYXNzZXMsIHthY3RpdmU6IG1ldHJpYy5zaXplICE9IFxcJ2JpZ1xcJ31dXCIgbmctY2xpY2s9XCJzZXRTaXplKFxcJ3NtYWxsXFwnKVwiPlNtYWxsPC9idXR0b24+IDxidXR0b24gdHlwZT1cImJ1dHRvblwiIG5nLWNsYXNzPVwiW2J0bkNsYXNzZXMsIHthY3RpdmU6IG1ldHJpYy5zaXplID09IFxcJ2JpZ1xcJ31dXCIgbmctY2xpY2s9XCJzZXRTaXplKFxcJ2JpZ1xcJylcIj5CaWc8L2J1dHRvbj4gPC9kaXY+IDxhIHRpdGxlPVwiUmVtb3ZlXCIgY2xhc3M9XCJidG4gYnRuLWRlZmF1bHQgYnRuLXhzIHJlbW92ZVwiIG5nLWNsaWNrPVwicmVtb3ZlTWV0cmljKClcIj48aSBjbGFzcz1cImZhIGZhLWNsb3NlXCIgLz48L2E+IDwvZGl2PiA8L2Rpdj4gPGRpdiBjbGFzcz1cInBhbmVsLWJvZHlcIj4gPHN2ZyAvPiA8L2Rpdj4gPC9kaXY+JyxcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBtZXRyaWM6IFwiPVwiLFxuICAgICAgd2luZG93OiBcIj1cIixcbiAgICAgIHJlbW92ZU1ldHJpYzogXCImXCIsXG4gICAgICBzZXRNZXRyaWNTaXplOiBcIj1cIixcbiAgICAgIGdldFZhbHVlczogXCImXCJcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgc2NvcGUuYnRuQ2xhc3NlcyA9IFsnYnRuJywgJ2J0bi1kZWZhdWx0JywgJ2J0bi14cyddO1xuICAgICAgc2NvcGUudmFsdWUgPSBudWxsO1xuICAgICAgc2NvcGUuZGF0YSA9IFtcbiAgICAgICAge1xuICAgICAgICAgIHZhbHVlczogc2NvcGUuZ2V0VmFsdWVzKClcbiAgICAgICAgfVxuICAgICAgXTtcbiAgICAgIHNjb3BlLm9wdGlvbnMgPSB7XG4gICAgICAgIHg6IGZ1bmN0aW9uKGQsIGkpIHtcbiAgICAgICAgICByZXR1cm4gZC54O1xuICAgICAgICB9LFxuICAgICAgICB5OiBmdW5jdGlvbihkLCBpKSB7XG4gICAgICAgICAgcmV0dXJuIGQueTtcbiAgICAgICAgfSxcbiAgICAgICAgeFRpY2tGb3JtYXQ6IGZ1bmN0aW9uKGQpIHtcbiAgICAgICAgICByZXR1cm4gZDMudGltZS5mb3JtYXQoJyVIOiVNOiVTJykobmV3IERhdGUoZCkpO1xuICAgICAgICB9LFxuICAgICAgICB5VGlja0Zvcm1hdDogZnVuY3Rpb24oZCkge1xuICAgICAgICAgIHZhciBhYnNELCBmb3VuZCwgcG93LCBzdGVwO1xuICAgICAgICAgIGZvdW5kID0gZmFsc2U7XG4gICAgICAgICAgcG93ID0gMDtcbiAgICAgICAgICBzdGVwID0gMTtcbiAgICAgICAgICBhYnNEID0gTWF0aC5hYnMoZCk7XG4gICAgICAgICAgd2hpbGUgKCFmb3VuZCAmJiBwb3cgPCA1MCkge1xuICAgICAgICAgICAgaWYgKE1hdGgucG93KDEwLCBwb3cpIDw9IGFic0QgJiYgYWJzRCA8IE1hdGgucG93KDEwLCBwb3cgKyBzdGVwKSkge1xuICAgICAgICAgICAgICBmb3VuZCA9IHRydWU7XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICBwb3cgKz0gc3RlcDtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGZvdW5kICYmIHBvdyA+IDYpIHtcbiAgICAgICAgICAgIHJldHVybiAoZCAvIE1hdGgucG93KDEwLCBwb3cpKSArIFwiRVwiICsgcG93O1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICByZXR1cm4gXCJcIiArIGQ7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgc2NvcGUuc2hvd0NoYXJ0ID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiBkMy5zZWxlY3QoZWxlbWVudC5maW5kKFwic3ZnXCIpWzBdKS5kYXR1bShzY29wZS5kYXRhKS50cmFuc2l0aW9uKCkuZHVyYXRpb24oMjUwKS5jYWxsKHNjb3BlLmNoYXJ0KTtcbiAgICAgIH07XG4gICAgICBzY29wZS5jaGFydCA9IG52Lm1vZGVscy5saW5lQ2hhcnQoKS5vcHRpb25zKHNjb3BlLm9wdGlvbnMpLnNob3dMZWdlbmQoZmFsc2UpLm1hcmdpbih7XG4gICAgICAgIHRvcDogMTUsXG4gICAgICAgIGxlZnQ6IDYwLFxuICAgICAgICBib3R0b206IDMwLFxuICAgICAgICByaWdodDogMzBcbiAgICAgIH0pO1xuICAgICAgc2NvcGUuY2hhcnQueUF4aXMuc2hvd01heE1pbihmYWxzZSk7XG4gICAgICBzY29wZS5jaGFydC50b29sdGlwLmhpZGVEZWxheSgwKTtcbiAgICAgIHNjb3BlLmNoYXJ0LnRvb2x0aXAuY29udGVudEdlbmVyYXRvcihmdW5jdGlvbihvYmopIHtcbiAgICAgICAgcmV0dXJuIFwiPHA+XCIgKyAoZDMudGltZS5mb3JtYXQoJyVIOiVNOiVTJykobmV3IERhdGUob2JqLnBvaW50LngpKSkgKyBcIiB8IFwiICsgb2JqLnBvaW50LnkgKyBcIjwvcD5cIjtcbiAgICAgIH0pO1xuICAgICAgbnYudXRpbHMud2luZG93UmVzaXplKHNjb3BlLmNoYXJ0LnVwZGF0ZSk7XG4gICAgICBzY29wZS5zZXRTaXplID0gZnVuY3Rpb24oc2l6ZSkge1xuICAgICAgICByZXR1cm4gc2NvcGUuc2V0TWV0cmljU2l6ZShzY29wZS5tZXRyaWMsIHNpemUpO1xuICAgICAgfTtcbiAgICAgIHNjb3BlLnNob3dDaGFydCgpO1xuICAgICAgc2NvcGUuJG9uKCdtZXRyaWNzOmRhdGE6dXBkYXRlJywgZnVuY3Rpb24oZXZlbnQsIHRpbWVzdGFtcCwgZGF0YSkge1xuICAgICAgICBzY29wZS52YWx1ZSA9IHBhcnNlRmxvYXQoZGF0YVtzY29wZS5tZXRyaWMuaWRdKTtcbiAgICAgICAgc2NvcGUuZGF0YVswXS52YWx1ZXMucHVzaCh7XG4gICAgICAgICAgeDogdGltZXN0YW1wLFxuICAgICAgICAgIHk6IHNjb3BlLnZhbHVlXG4gICAgICAgIH0pO1xuICAgICAgICBpZiAoc2NvcGUuZGF0YVswXS52YWx1ZXMubGVuZ3RoID4gc2NvcGUud2luZG93KSB7XG4gICAgICAgICAgc2NvcGUuZGF0YVswXS52YWx1ZXMuc2hpZnQoKTtcbiAgICAgICAgfVxuICAgICAgICBzY29wZS5zaG93Q2hhcnQoKTtcbiAgICAgICAgc2NvcGUuY2hhcnQuY2xlYXJIaWdobGlnaHRzKCk7XG4gICAgICAgIHJldHVybiBzY29wZS5jaGFydC50b29sdGlwLmhpZGRlbih0cnVlKTtcbiAgICAgIH0pO1xuICAgICAgcmV0dXJuIGVsZW1lbnQuZmluZChcIi5tZXRyaWMtdGl0bGVcIikucXRpcCh7XG4gICAgICAgIGNvbnRlbnQ6IHtcbiAgICAgICAgICB0ZXh0OiBzY29wZS5tZXRyaWMuaWRcbiAgICAgICAgfSxcbiAgICAgICAgcG9zaXRpb246IHtcbiAgICAgICAgICBteTogJ2JvdHRvbSBsZWZ0JyxcbiAgICAgICAgICBhdDogJ3RvcCBsZWZ0J1xuICAgICAgICB9LFxuICAgICAgICBzdHlsZToge1xuICAgICAgICAgIGNsYXNzZXM6ICdxdGlwLWxpZ2h0IHF0aXAtdGltZWxpbmUtYmFyJ1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnTWV0cmljc1NlcnZpY2UnLCAoJGh0dHAsICRxLCBmbGlua0NvbmZpZywgJGludGVydmFsKSAtPlxuICBAbWV0cmljcyA9IHt9XG4gIEB2YWx1ZXMgPSB7fVxuICBAd2F0Y2hlZCA9IHt9XG4gIEBvYnNlcnZlciA9IHtcbiAgICBqb2JpZDogbnVsbFxuICAgIG5vZGVpZDogbnVsbFxuICAgIGNhbGxiYWNrOiBudWxsXG4gIH1cblxuICBAcmVmcmVzaCA9ICRpbnRlcnZhbCA9PlxuICAgIGFuZ3VsYXIuZm9yRWFjaCBAbWV0cmljcywgKHZlcnRpY2VzLCBqb2JpZCkgPT5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCB2ZXJ0aWNlcywgKG1ldHJpY3MsIG5vZGVpZCkgPT5cbiAgICAgICAgbmFtZXMgPSBbXVxuICAgICAgICBhbmd1bGFyLmZvckVhY2ggbWV0cmljcywgKG1ldHJpYywgaW5kZXgpID0+XG4gICAgICAgICAgbmFtZXMucHVzaCBtZXRyaWMuaWRcblxuICAgICAgICBpZiBuYW1lcy5sZW5ndGggPiAwXG4gICAgICAgICAgQGdldE1ldHJpY3Moam9iaWQsIG5vZGVpZCwgbmFtZXMpLnRoZW4gKHZhbHVlcykgPT5cbiAgICAgICAgICAgIGlmIGpvYmlkID09IEBvYnNlcnZlci5qb2JpZCAmJiBub2RlaWQgPT0gQG9ic2VydmVyLm5vZGVpZFxuICAgICAgICAgICAgICBAb2JzZXJ2ZXIuY2FsbGJhY2sodmFsdWVzKSBpZiBAb2JzZXJ2ZXIuY2FsbGJhY2tcblxuXG4gICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXG5cbiAgQHJlZ2lzdGVyT2JzZXJ2ZXIgPSAoam9iaWQsIG5vZGVpZCwgY2FsbGJhY2spIC0+XG4gICAgQG9ic2VydmVyLmpvYmlkID0gam9iaWRcbiAgICBAb2JzZXJ2ZXIubm9kZWlkID0gbm9kZWlkXG4gICAgQG9ic2VydmVyLmNhbGxiYWNrID0gY2FsbGJhY2tcblxuICBAdW5SZWdpc3Rlck9ic2VydmVyID0gLT5cbiAgICBAb2JzZXJ2ZXIgPSB7XG4gICAgICBqb2JpZDogbnVsbFxuICAgICAgbm9kZWlkOiBudWxsXG4gICAgICBjYWxsYmFjazogbnVsbFxuICAgIH1cblxuICBAc2V0dXBNZXRyaWNzID0gKGpvYmlkLCB2ZXJ0aWNlcykgLT5cbiAgICBAc2V0dXBMUygpXG5cbiAgICBAd2F0Y2hlZFtqb2JpZF0gPSBbXVxuICAgIGFuZ3VsYXIuZm9yRWFjaCB2ZXJ0aWNlcywgKHYsIGspID0+XG4gICAgICBAd2F0Y2hlZFtqb2JpZF0ucHVzaCh2LmlkKSBpZiB2LmlkXG5cbiAgQGdldFdpbmRvdyA9IC0+XG4gICAgMTAwXG5cbiAgQHNldHVwTFMgPSAtPlxuICAgIGlmICFzZXNzaW9uU3RvcmFnZS5mbGlua01ldHJpY3M/XG4gICAgICBAc2F2ZVNldHVwKClcblxuICAgIEBtZXRyaWNzID0gSlNPTi5wYXJzZShzZXNzaW9uU3RvcmFnZS5mbGlua01ldHJpY3MpXG5cbiAgQHNhdmVTZXR1cCA9IC0+XG4gICAgc2Vzc2lvblN0b3JhZ2UuZmxpbmtNZXRyaWNzID0gSlNPTi5zdHJpbmdpZnkoQG1ldHJpY3MpXG5cbiAgQHNhdmVWYWx1ZSA9IChqb2JpZCwgbm9kZWlkLCB2YWx1ZSkgLT5cbiAgICB1bmxlc3MgQHZhbHVlc1tqb2JpZF0/XG4gICAgICBAdmFsdWVzW2pvYmlkXSA9IHt9XG5cbiAgICB1bmxlc3MgQHZhbHVlc1tqb2JpZF1bbm9kZWlkXT9cbiAgICAgIEB2YWx1ZXNbam9iaWRdW25vZGVpZF0gPSBbXVxuXG4gICAgQHZhbHVlc1tqb2JpZF1bbm9kZWlkXS5wdXNoKHZhbHVlKVxuXG4gICAgaWYgQHZhbHVlc1tqb2JpZF1bbm9kZWlkXS5sZW5ndGggPiBAZ2V0V2luZG93KClcbiAgICAgIEB2YWx1ZXNbam9iaWRdW25vZGVpZF0uc2hpZnQoKVxuXG4gIEBnZXRWYWx1ZXMgPSAoam9iaWQsIG5vZGVpZCwgbWV0cmljaWQpIC0+XG4gICAgcmV0dXJuIFtdIHVubGVzcyBAdmFsdWVzW2pvYmlkXT9cbiAgICByZXR1cm4gW10gdW5sZXNzIEB2YWx1ZXNbam9iaWRdW25vZGVpZF0/XG5cbiAgICByZXN1bHRzID0gW11cbiAgICBhbmd1bGFyLmZvckVhY2ggQHZhbHVlc1tqb2JpZF1bbm9kZWlkXSwgKHYsIGspID0+XG4gICAgICBpZiB2LnZhbHVlc1ttZXRyaWNpZF0/XG4gICAgICAgIHJlc3VsdHMucHVzaCB7XG4gICAgICAgICAgeDogdi50aW1lc3RhbXBcbiAgICAgICAgICB5OiB2LnZhbHVlc1ttZXRyaWNpZF1cbiAgICAgICAgfVxuXG4gICAgcmVzdWx0c1xuXG4gIEBzZXR1cExTRm9yID0gKGpvYmlkLCBub2RlaWQpIC0+XG4gICAgaWYgIUBtZXRyaWNzW2pvYmlkXT9cbiAgICAgIEBtZXRyaWNzW2pvYmlkXSA9IHt9XG5cbiAgICBpZiAhQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0/XG4gICAgICBAbWV0cmljc1tqb2JpZF1bbm9kZWlkXSA9IFtdXG5cbiAgQGFkZE1ldHJpYyA9IChqb2JpZCwgbm9kZWlkLCBtZXRyaWNpZCkgLT5cbiAgICBAc2V0dXBMU0Zvcihqb2JpZCwgbm9kZWlkKVxuXG4gICAgQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0ucHVzaCh7aWQ6IG1ldHJpY2lkLCBzaXplOiAnc21hbGwnfSlcblxuICAgIEBzYXZlU2V0dXAoKVxuXG4gIEByZW1vdmVNZXRyaWMgPSAoam9iaWQsIG5vZGVpZCwgbWV0cmljKSA9PlxuICAgIGlmIEBtZXRyaWNzW2pvYmlkXVtub2RlaWRdP1xuICAgICAgaSA9IEBtZXRyaWNzW2pvYmlkXVtub2RlaWRdLmluZGV4T2YobWV0cmljKVxuICAgICAgaSA9IF8uZmluZEluZGV4KEBtZXRyaWNzW2pvYmlkXVtub2RlaWRdLCB7IGlkOiBtZXRyaWMgfSkgaWYgaSA9PSAtMVxuXG4gICAgICBAbWV0cmljc1tqb2JpZF1bbm9kZWlkXS5zcGxpY2UoaSwgMSkgaWYgaSAhPSAtMVxuXG4gICAgICBAc2F2ZVNldHVwKClcblxuICBAc2V0TWV0cmljU2l6ZSA9IChqb2JpZCwgbm9kZWlkLCBtZXRyaWMsIHNpemUpID0+XG4gICAgaWYgQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0/XG4gICAgICBpID0gQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0uaW5kZXhPZihtZXRyaWMuaWQpXG4gICAgICBpID0gXy5maW5kSW5kZXgoQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0sIHsgaWQ6IG1ldHJpYy5pZCB9KSBpZiBpID09IC0xXG5cbiAgICAgIEBtZXRyaWNzW2pvYmlkXVtub2RlaWRdW2ldID0geyBpZDogbWV0cmljLmlkLCBzaXplOiBzaXplIH0gaWYgaSAhPSAtMVxuXG4gICAgICBAc2F2ZVNldHVwKClcblxuICBAb3JkZXJNZXRyaWNzID0gKGpvYmlkLCBub2RlaWQsIGl0ZW0sIGluZGV4KSAtPlxuICAgIEBzZXR1cExTRm9yKGpvYmlkLCBub2RlaWQpXG5cbiAgICBhbmd1bGFyLmZvckVhY2ggQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0sICh2LCBrKSA9PlxuICAgICAgaWYgdi5pZCA9PSBpdGVtLmlkXG4gICAgICAgIEBtZXRyaWNzW2pvYmlkXVtub2RlaWRdLnNwbGljZShrLCAxKVxuICAgICAgICBpZiBrIDwgaW5kZXhcbiAgICAgICAgICBpbmRleCA9IGluZGV4IC0gMVxuXG4gICAgQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0uc3BsaWNlKGluZGV4LCAwLCBpdGVtKVxuXG4gICAgQHNhdmVTZXR1cCgpXG5cbiAgQGdldE1ldHJpY3NTZXR1cCA9IChqb2JpZCwgbm9kZWlkKSA9PlxuICAgIHtcbiAgICAgIG5hbWVzOiBfLm1hcChAbWV0cmljc1tqb2JpZF1bbm9kZWlkXSwgKHZhbHVlKSA9PlxuICAgICAgICBpZiBfLmlzU3RyaW5nKHZhbHVlKSB0aGVuIHsgaWQ6IHZhbHVlLCBzaXplOiBcInNtYWxsXCIgfSBlbHNlIHZhbHVlXG4gICAgICApXG4gICAgfVxuXG4gIEBnZXRBdmFpbGFibGVNZXRyaWNzID0gKGpvYmlkLCBub2RlaWQpID0+XG4gICAgQHNldHVwTFNGb3Ioam9iaWQsIG5vZGVpZClcblxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIvdmVydGljZXMvXCIgKyBub2RlaWQgKyBcIi9tZXRyaWNzXCJcbiAgICAuc3VjY2VzcyAoZGF0YSkgPT5cbiAgICAgIHJlc3VsdHMgPSBbXVxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEsICh2LCBrKSA9PlxuICAgICAgICBpID0gQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0uaW5kZXhPZih2LmlkKVxuICAgICAgICBpID0gXy5maW5kSW5kZXgoQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0sIHsgaWQ6IHYuaWQgfSkgaWYgaSA9PSAtMVxuXG4gICAgICAgIGlmIGkgPT0gLTFcbiAgICAgICAgICByZXN1bHRzLnB1c2godilcblxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShyZXN1bHRzKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRBbGxBdmFpbGFibGVNZXRyaWNzID0gKGpvYmlkLCBub2RlaWQpID0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0aWNlcy9cIiArIG5vZGVpZCArIFwiL21ldHJpY3NcIlxuICAgIC5zdWNjZXNzIChkYXRhKSA9PlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRNZXRyaWNzID0gKGpvYmlkLCBub2RlaWQsIG1ldHJpY0lkcykgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgIGlkcyA9IG1ldHJpY0lkcy5qb2luKFwiLFwiKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIvdmVydGljZXMvXCIgKyBub2RlaWQgKyBcIi9tZXRyaWNzP2dldD1cIiArIGlkc1xuICAgIC5zdWNjZXNzIChkYXRhKSA9PlxuICAgICAgcmVzdWx0ID0ge31cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLCAodiwgaykgLT5cbiAgICAgICAgcmVzdWx0W3YuaWRdID0gcGFyc2VJbnQodi52YWx1ZSlcblxuICAgICAgbmV3VmFsdWUgPSB7XG4gICAgICAgIHRpbWVzdGFtcDogRGF0ZS5ub3coKVxuICAgICAgICB2YWx1ZXM6IHJlc3VsdFxuICAgICAgfVxuICAgICAgQHNhdmVWYWx1ZShqb2JpZCwgbm9kZWlkLCBuZXdWYWx1ZSlcbiAgICAgIGRlZmVycmVkLnJlc29sdmUobmV3VmFsdWUpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQHNldHVwTFMoKVxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ01ldHJpY3NTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsICRxLCBmbGlua0NvbmZpZywgJGludGVydmFsKSB7XG4gIHRoaXMubWV0cmljcyA9IHt9O1xuICB0aGlzLnZhbHVlcyA9IHt9O1xuICB0aGlzLndhdGNoZWQgPSB7fTtcbiAgdGhpcy5vYnNlcnZlciA9IHtcbiAgICBqb2JpZDogbnVsbCxcbiAgICBub2RlaWQ6IG51bGwsXG4gICAgY2FsbGJhY2s6IG51bGxcbiAgfTtcbiAgdGhpcy5yZWZyZXNoID0gJGludGVydmFsKChmdW5jdGlvbihfdGhpcykge1xuICAgIHJldHVybiBmdW5jdGlvbigpIHtcbiAgICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2goX3RoaXMubWV0cmljcywgZnVuY3Rpb24odmVydGljZXMsIGpvYmlkKSB7XG4gICAgICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2godmVydGljZXMsIGZ1bmN0aW9uKG1ldHJpY3MsIG5vZGVpZCkge1xuICAgICAgICAgIHZhciBuYW1lcztcbiAgICAgICAgICBuYW1lcyA9IFtdO1xuICAgICAgICAgIGFuZ3VsYXIuZm9yRWFjaChtZXRyaWNzLCBmdW5jdGlvbihtZXRyaWMsIGluZGV4KSB7XG4gICAgICAgICAgICByZXR1cm4gbmFtZXMucHVzaChtZXRyaWMuaWQpO1xuICAgICAgICAgIH0pO1xuICAgICAgICAgIGlmIChuYW1lcy5sZW5ndGggPiAwKSB7XG4gICAgICAgICAgICByZXR1cm4gX3RoaXMuZ2V0TWV0cmljcyhqb2JpZCwgbm9kZWlkLCBuYW1lcykudGhlbihmdW5jdGlvbih2YWx1ZXMpIHtcbiAgICAgICAgICAgICAgaWYgKGpvYmlkID09PSBfdGhpcy5vYnNlcnZlci5qb2JpZCAmJiBub2RlaWQgPT09IF90aGlzLm9ic2VydmVyLm5vZGVpZCkge1xuICAgICAgICAgICAgICAgIGlmIChfdGhpcy5vYnNlcnZlci5jYWxsYmFjaykge1xuICAgICAgICAgICAgICAgICAgcmV0dXJuIF90aGlzLm9ic2VydmVyLmNhbGxiYWNrKHZhbHVlcyk7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgfSk7XG4gICAgfTtcbiAgfSkodGhpcyksIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIHRoaXMucmVnaXN0ZXJPYnNlcnZlciA9IGZ1bmN0aW9uKGpvYmlkLCBub2RlaWQsIGNhbGxiYWNrKSB7XG4gICAgdGhpcy5vYnNlcnZlci5qb2JpZCA9IGpvYmlkO1xuICAgIHRoaXMub2JzZXJ2ZXIubm9kZWlkID0gbm9kZWlkO1xuICAgIHJldHVybiB0aGlzLm9ic2VydmVyLmNhbGxiYWNrID0gY2FsbGJhY2s7XG4gIH07XG4gIHRoaXMudW5SZWdpc3Rlck9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIHRoaXMub2JzZXJ2ZXIgPSB7XG4gICAgICBqb2JpZDogbnVsbCxcbiAgICAgIG5vZGVpZDogbnVsbCxcbiAgICAgIGNhbGxiYWNrOiBudWxsXG4gICAgfTtcbiAgfTtcbiAgdGhpcy5zZXR1cE1ldHJpY3MgPSBmdW5jdGlvbihqb2JpZCwgdmVydGljZXMpIHtcbiAgICB0aGlzLnNldHVwTFMoKTtcbiAgICB0aGlzLndhdGNoZWRbam9iaWRdID0gW107XG4gICAgcmV0dXJuIGFuZ3VsYXIuZm9yRWFjaCh2ZXJ0aWNlcywgKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24odiwgaykge1xuICAgICAgICBpZiAodi5pZCkge1xuICAgICAgICAgIHJldHVybiBfdGhpcy53YXRjaGVkW2pvYmlkXS5wdXNoKHYuaWQpO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgfTtcbiAgdGhpcy5nZXRXaW5kb3cgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gMTAwO1xuICB9O1xuICB0aGlzLnNldHVwTFMgPSBmdW5jdGlvbigpIHtcbiAgICBpZiAoc2Vzc2lvblN0b3JhZ2UuZmxpbmtNZXRyaWNzID09IG51bGwpIHtcbiAgICAgIHRoaXMuc2F2ZVNldHVwKCk7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLm1ldHJpY3MgPSBKU09OLnBhcnNlKHNlc3Npb25TdG9yYWdlLmZsaW5rTWV0cmljcyk7XG4gIH07XG4gIHRoaXMuc2F2ZVNldHVwID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIHNlc3Npb25TdG9yYWdlLmZsaW5rTWV0cmljcyA9IEpTT04uc3RyaW5naWZ5KHRoaXMubWV0cmljcyk7XG4gIH07XG4gIHRoaXMuc2F2ZVZhbHVlID0gZnVuY3Rpb24oam9iaWQsIG5vZGVpZCwgdmFsdWUpIHtcbiAgICBpZiAodGhpcy52YWx1ZXNbam9iaWRdID09IG51bGwpIHtcbiAgICAgIHRoaXMudmFsdWVzW2pvYmlkXSA9IHt9O1xuICAgIH1cbiAgICBpZiAodGhpcy52YWx1ZXNbam9iaWRdW25vZGVpZF0gPT0gbnVsbCkge1xuICAgICAgdGhpcy52YWx1ZXNbam9iaWRdW25vZGVpZF0gPSBbXTtcbiAgICB9XG4gICAgdGhpcy52YWx1ZXNbam9iaWRdW25vZGVpZF0ucHVzaCh2YWx1ZSk7XG4gICAgaWYgKHRoaXMudmFsdWVzW2pvYmlkXVtub2RlaWRdLmxlbmd0aCA+IHRoaXMuZ2V0V2luZG93KCkpIHtcbiAgICAgIHJldHVybiB0aGlzLnZhbHVlc1tqb2JpZF1bbm9kZWlkXS5zaGlmdCgpO1xuICAgIH1cbiAgfTtcbiAgdGhpcy5nZXRWYWx1ZXMgPSBmdW5jdGlvbihqb2JpZCwgbm9kZWlkLCBtZXRyaWNpZCkge1xuICAgIHZhciByZXN1bHRzO1xuICAgIGlmICh0aGlzLnZhbHVlc1tqb2JpZF0gPT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFtdO1xuICAgIH1cbiAgICBpZiAodGhpcy52YWx1ZXNbam9iaWRdW25vZGVpZF0gPT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFtdO1xuICAgIH1cbiAgICByZXN1bHRzID0gW107XG4gICAgYW5ndWxhci5mb3JFYWNoKHRoaXMudmFsdWVzW2pvYmlkXVtub2RlaWRdLCAoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbih2LCBrKSB7XG4gICAgICAgIGlmICh2LnZhbHVlc1ttZXRyaWNpZF0gIT0gbnVsbCkge1xuICAgICAgICAgIHJldHVybiByZXN1bHRzLnB1c2goe1xuICAgICAgICAgICAgeDogdi50aW1lc3RhbXAsXG4gICAgICAgICAgICB5OiB2LnZhbHVlc1ttZXRyaWNpZF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIHJlc3VsdHM7XG4gIH07XG4gIHRoaXMuc2V0dXBMU0ZvciA9IGZ1bmN0aW9uKGpvYmlkLCBub2RlaWQpIHtcbiAgICBpZiAodGhpcy5tZXRyaWNzW2pvYmlkXSA9PSBudWxsKSB7XG4gICAgICB0aGlzLm1ldHJpY3Nbam9iaWRdID0ge307XG4gICAgfVxuICAgIGlmICh0aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF0gPT0gbnVsbCkge1xuICAgICAgcmV0dXJuIHRoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXSA9IFtdO1xuICAgIH1cbiAgfTtcbiAgdGhpcy5hZGRNZXRyaWMgPSBmdW5jdGlvbihqb2JpZCwgbm9kZWlkLCBtZXRyaWNpZCkge1xuICAgIHRoaXMuc2V0dXBMU0Zvcihqb2JpZCwgbm9kZWlkKTtcbiAgICB0aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF0ucHVzaCh7XG4gICAgICBpZDogbWV0cmljaWQsXG4gICAgICBzaXplOiAnc21hbGwnXG4gICAgfSk7XG4gICAgcmV0dXJuIHRoaXMuc2F2ZVNldHVwKCk7XG4gIH07XG4gIHRoaXMucmVtb3ZlTWV0cmljID0gKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgcmV0dXJuIGZ1bmN0aW9uKGpvYmlkLCBub2RlaWQsIG1ldHJpYykge1xuICAgICAgdmFyIGk7XG4gICAgICBpZiAoX3RoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXSAhPSBudWxsKSB7XG4gICAgICAgIGkgPSBfdGhpcy5tZXRyaWNzW2pvYmlkXVtub2RlaWRdLmluZGV4T2YobWV0cmljKTtcbiAgICAgICAgaWYgKGkgPT09IC0xKSB7XG4gICAgICAgICAgaSA9IF8uZmluZEluZGV4KF90aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF0sIHtcbiAgICAgICAgICAgIGlkOiBtZXRyaWNcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoaSAhPT0gLTEpIHtcbiAgICAgICAgICBfdGhpcy5tZXRyaWNzW2pvYmlkXVtub2RlaWRdLnNwbGljZShpLCAxKTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gX3RoaXMuc2F2ZVNldHVwKCk7XG4gICAgICB9XG4gICAgfTtcbiAgfSkodGhpcyk7XG4gIHRoaXMuc2V0TWV0cmljU2l6ZSA9IChmdW5jdGlvbihfdGhpcykge1xuICAgIHJldHVybiBmdW5jdGlvbihqb2JpZCwgbm9kZWlkLCBtZXRyaWMsIHNpemUpIHtcbiAgICAgIHZhciBpO1xuICAgICAgaWYgKF90aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF0gIT0gbnVsbCkge1xuICAgICAgICBpID0gX3RoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXS5pbmRleE9mKG1ldHJpYy5pZCk7XG4gICAgICAgIGlmIChpID09PSAtMSkge1xuICAgICAgICAgIGkgPSBfLmZpbmRJbmRleChfdGhpcy5tZXRyaWNzW2pvYmlkXVtub2RlaWRdLCB7XG4gICAgICAgICAgICBpZDogbWV0cmljLmlkXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGkgIT09IC0xKSB7XG4gICAgICAgICAgX3RoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXVtpXSA9IHtcbiAgICAgICAgICAgIGlkOiBtZXRyaWMuaWQsXG4gICAgICAgICAgICBzaXplOiBzaXplXG4gICAgICAgICAgfTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gX3RoaXMuc2F2ZVNldHVwKCk7XG4gICAgICB9XG4gICAgfTtcbiAgfSkodGhpcyk7XG4gIHRoaXMub3JkZXJNZXRyaWNzID0gZnVuY3Rpb24oam9iaWQsIG5vZGVpZCwgaXRlbSwgaW5kZXgpIHtcbiAgICB0aGlzLnNldHVwTFNGb3Ioam9iaWQsIG5vZGVpZCk7XG4gICAgYW5ndWxhci5mb3JFYWNoKHRoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXSwgKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24odiwgaykge1xuICAgICAgICBpZiAodi5pZCA9PT0gaXRlbS5pZCkge1xuICAgICAgICAgIF90aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF0uc3BsaWNlKGssIDEpO1xuICAgICAgICAgIGlmIChrIDwgaW5kZXgpIHtcbiAgICAgICAgICAgIHJldHVybiBpbmRleCA9IGluZGV4IC0gMTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHRoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXS5zcGxpY2UoaW5kZXgsIDAsIGl0ZW0pO1xuICAgIHJldHVybiB0aGlzLnNhdmVTZXR1cCgpO1xuICB9O1xuICB0aGlzLmdldE1ldHJpY3NTZXR1cCA9IChmdW5jdGlvbihfdGhpcykge1xuICAgIHJldHVybiBmdW5jdGlvbihqb2JpZCwgbm9kZWlkKSB7XG4gICAgICByZXR1cm4ge1xuICAgICAgICBuYW1lczogXy5tYXAoX3RoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXSwgZnVuY3Rpb24odmFsdWUpIHtcbiAgICAgICAgICBpZiAoXy5pc1N0cmluZyh2YWx1ZSkpIHtcbiAgICAgICAgICAgIHJldHVybiB7XG4gICAgICAgICAgICAgIGlkOiB2YWx1ZSxcbiAgICAgICAgICAgICAgc2l6ZTogXCJzbWFsbFwiXG4gICAgICAgICAgICB9O1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICByZXR1cm4gdmFsdWU7XG4gICAgICAgICAgfVxuICAgICAgICB9KVxuICAgICAgfTtcbiAgICB9O1xuICB9KSh0aGlzKTtcbiAgdGhpcy5nZXRBdmFpbGFibGVNZXRyaWNzID0gKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgcmV0dXJuIGZ1bmN0aW9uKGpvYmlkLCBub2RlaWQpIHtcbiAgICAgIHZhciBkZWZlcnJlZDtcbiAgICAgIF90aGlzLnNldHVwTFNGb3Ioam9iaWQsIG5vZGVpZCk7XG4gICAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0aWNlcy9cIiArIG5vZGVpZCArIFwiL21ldHJpY3NcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciByZXN1bHRzO1xuICAgICAgICByZXN1bHRzID0gW107XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLCBmdW5jdGlvbih2LCBrKSB7XG4gICAgICAgICAgdmFyIGk7XG4gICAgICAgICAgaSA9IF90aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF0uaW5kZXhPZih2LmlkKTtcbiAgICAgICAgICBpZiAoaSA9PT0gLTEpIHtcbiAgICAgICAgICAgIGkgPSBfLmZpbmRJbmRleChfdGhpcy5tZXRyaWNzW2pvYmlkXVtub2RlaWRdLCB7XG4gICAgICAgICAgICAgIGlkOiB2LmlkXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGkgPT09IC0xKSB7XG4gICAgICAgICAgICByZXR1cm4gcmVzdWx0cy5wdXNoKHYpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKHJlc3VsdHMpO1xuICAgICAgfSk7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgICB9O1xuICB9KSh0aGlzKTtcbiAgdGhpcy5nZXRBbGxBdmFpbGFibGVNZXRyaWNzID0gKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgcmV0dXJuIGZ1bmN0aW9uKGpvYmlkLCBub2RlaWQpIHtcbiAgICAgIHZhciBkZWZlcnJlZDtcbiAgICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRpY2VzL1wiICsgbm9kZWlkICsgXCIvbWV0cmljc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgICB9KTtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICAgIH07XG4gIH0pKHRoaXMpO1xuICB0aGlzLmdldE1ldHJpY3MgPSBmdW5jdGlvbihqb2JpZCwgbm9kZWlkLCBtZXRyaWNJZHMpIHtcbiAgICB2YXIgZGVmZXJyZWQsIGlkcztcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgaWRzID0gbWV0cmljSWRzLmpvaW4oXCIsXCIpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRpY2VzL1wiICsgbm9kZWlkICsgXCIvbWV0cmljcz9nZXQ9XCIgKyBpZHMpLnN1Y2Nlc3MoKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgbmV3VmFsdWUsIHJlc3VsdDtcbiAgICAgICAgcmVzdWx0ID0ge307XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLCBmdW5jdGlvbih2LCBrKSB7XG4gICAgICAgICAgcmV0dXJuIHJlc3VsdFt2LmlkXSA9IHBhcnNlSW50KHYudmFsdWUpO1xuICAgICAgICB9KTtcbiAgICAgICAgbmV3VmFsdWUgPSB7XG4gICAgICAgICAgdGltZXN0YW1wOiBEYXRlLm5vdygpLFxuICAgICAgICAgIHZhbHVlczogcmVzdWx0XG4gICAgICAgIH07XG4gICAgICAgIF90aGlzLnNhdmVWYWx1ZShqb2JpZCwgbm9kZWlkLCBuZXdWYWx1ZSk7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKG5ld1ZhbHVlKTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLnNldHVwTFMoKTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLmNvbnRyb2xsZXIgJ092ZXJ2aWV3Q29udHJvbGxlcicsICgkc2NvcGUsIE92ZXJ2aWV3U2VydmljZSwgSm9ic1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIC0+XHJcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cclxuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxyXG4gICAgJHNjb3BlLmZpbmlzaGVkSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJylcclxuXHJcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcclxuXHJcbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcclxuXHJcbiAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhXHJcblxyXG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cclxuICAgIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhXHJcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cclxuXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5jb250cm9sbGVyKCdPdmVydmlld0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIE92ZXJ2aWV3U2VydmljZSwgSm9ic1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgdmFyIHJlZnJlc2g7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKTtcbiAgICByZXR1cm4gJHNjb3BlLmZpbmlzaGVkSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbiAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhO1xuICB9KTtcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5vdmVydmlldyA9IGRhdGE7XG4gICAgfSk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4uc2VydmljZSAnT3ZlcnZpZXdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XHJcbiAgb3ZlcnZpZXcgPSB7fVxyXG5cclxuICBAbG9hZE92ZXJ2aWV3ID0gLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIm92ZXJ2aWV3XCIpXHJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XHJcbiAgICAgIG92ZXJ2aWV3ID0gZGF0YVxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ092ZXJ2aWV3U2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdmFyIG92ZXJ2aWV3O1xuICBvdmVydmlldyA9IHt9O1xuICB0aGlzLmxvYWRPdmVydmlldyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwib3ZlcnZpZXdcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgb3ZlcnZpZXcgPSBkYXRhO1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcclxuXHJcbi5jb250cm9sbGVyICdKb2JTdWJtaXRDb250cm9sbGVyJywgKCRzY29wZSwgSm9iU3VibWl0U2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZywgJHN0YXRlLCAkbG9jYXRpb24pIC0+XHJcbiAgJHNjb3BlLnlhcm4gPSAkbG9jYXRpb24uYWJzVXJsKCkuaW5kZXhPZihcIi9wcm94eS9hcHBsaWNhdGlvbl9cIikgIT0gLTFcclxuICAkc2NvcGUubG9hZExpc3QgPSAoKSAtPlxyXG4gICAgSm9iU3VibWl0U2VydmljZS5sb2FkSmFyTGlzdCgpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS5hZGRyZXNzID0gZGF0YS5hZGRyZXNzXHJcbiAgICAgICRzY29wZS5ub2FjY2VzcyA9IGRhdGEuZXJyb3JcclxuICAgICAgJHNjb3BlLmphcnMgPSBkYXRhLmZpbGVzXHJcblxyXG4gICRzY29wZS5kZWZhdWx0U3RhdGUgPSAoKSAtPlxyXG4gICAgJHNjb3BlLnBsYW4gPSBudWxsXHJcbiAgICAkc2NvcGUuZXJyb3IgPSBudWxsXHJcbiAgICAkc2NvcGUuc3RhdGUgPSB7XHJcbiAgICAgIHNlbGVjdGVkOiBudWxsLFxyXG4gICAgICBwYXJhbGxlbGlzbTogXCJcIixcclxuICAgICAgc2F2ZXBvaW50UGF0aDogXCJcIixcclxuICAgICAgYWxsb3dOb25SZXN0b3JlZFN0YXRlOiBmYWxzZVxyXG4gICAgICAnZW50cnktY2xhc3MnOiBcIlwiLFxyXG4gICAgICAncHJvZ3JhbS1hcmdzJzogXCJcIixcclxuICAgICAgJ3BsYW4tYnV0dG9uJzogXCJTaG93IFBsYW5cIixcclxuICAgICAgJ3N1Ym1pdC1idXR0b24nOiBcIlN1Ym1pdFwiLFxyXG4gICAgICAnYWN0aW9uLXRpbWUnOiAwXHJcbiAgICB9XHJcblxyXG4gICRzY29wZS5kZWZhdWx0U3RhdGUoKVxyXG4gICRzY29wZS51cGxvYWRlciA9IHt9XHJcbiAgJHNjb3BlLmxvYWRMaXN0KClcclxuXHJcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbCAtPlxyXG4gICAgJHNjb3BlLmxvYWRMaXN0KClcclxuICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxyXG5cclxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XHJcbiAgICAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpXHJcblxyXG4gICRzY29wZS5zZWxlY3RKYXIgPSAoaWQpIC0+XHJcbiAgICBpZiAkc2NvcGUuc3RhdGUuc2VsZWN0ZWQgPT0gaWRcclxuICAgICAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpXHJcbiAgICBlbHNlXHJcbiAgICAgICRzY29wZS5kZWZhdWx0U3RhdGUoKVxyXG4gICAgICAkc2NvcGUuc3RhdGUuc2VsZWN0ZWQgPSBpZFxyXG5cclxuICAkc2NvcGUuZGVsZXRlSmFyID0gKGV2ZW50LCBpZCkgLT5cclxuICAgIGlmICRzY29wZS5zdGF0ZS5zZWxlY3RlZCA9PSBpZFxyXG4gICAgICAkc2NvcGUuZGVmYXVsdFN0YXRlKClcclxuICAgIGFuZ3VsYXIuZWxlbWVudChldmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImZhLXJlbW92ZVwiKS5hZGRDbGFzcyhcImZhLXNwaW4gZmEtc3Bpbm5lclwiKVxyXG4gICAgSm9iU3VibWl0U2VydmljZS5kZWxldGVKYXIoaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChldmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImZhLXNwaW4gZmEtc3Bpbm5lclwiKS5hZGRDbGFzcyhcImZhLXJlbW92ZVwiKVxyXG4gICAgICBpZiBkYXRhLmVycm9yP1xyXG4gICAgICAgIGFsZXJ0KGRhdGEuZXJyb3IpXHJcblxyXG4gICRzY29wZS5sb2FkRW50cnlDbGFzcyA9IChuYW1lKSAtPlxyXG4gICAgJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddID0gbmFtZVxyXG5cclxuICAkc2NvcGUuZ2V0UGxhbiA9ICgpIC0+XHJcbiAgICBpZiAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPT0gXCJTaG93IFBsYW5cIlxyXG4gICAgICBhY3Rpb24gPSBuZXcgRGF0ZSgpLmdldFRpbWUoKVxyXG4gICAgICAkc2NvcGUuc3RhdGVbJ2FjdGlvbi10aW1lJ10gPSBhY3Rpb25cclxuICAgICAgJHNjb3BlLnN0YXRlWydzdWJtaXQtYnV0dG9uJ10gPSBcIlN1Ym1pdFwiXHJcbiAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiR2V0dGluZyBQbGFuXCJcclxuICAgICAgJHNjb3BlLmVycm9yID0gbnVsbFxyXG4gICAgICAkc2NvcGUucGxhbiA9IG51bGxcclxuICAgICAgSm9iU3VibWl0U2VydmljZS5nZXRQbGFuKFxyXG4gICAgICAgICRzY29wZS5zdGF0ZS5zZWxlY3RlZCwge1xyXG4gICAgICAgICAgJ2VudHJ5LWNsYXNzJzogJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddLFxyXG4gICAgICAgICAgcGFyYWxsZWxpc206ICRzY29wZS5zdGF0ZS5wYXJhbGxlbGlzbSxcclxuICAgICAgICAgICdwcm9ncmFtLWFyZ3MnOiAkc2NvcGUuc3RhdGVbJ3Byb2dyYW0tYXJncyddXHJcbiAgICAgICAgfVxyXG4gICAgICApLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICAgaWYgYWN0aW9uID09ICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXVxyXG4gICAgICAgICAgJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID0gXCJTaG93IFBsYW5cIlxyXG4gICAgICAgICAgJHNjb3BlLmVycm9yID0gZGF0YS5lcnJvclxyXG4gICAgICAgICAgJHNjb3BlLnBsYW4gPSBkYXRhLnBsYW5cclxuXHJcbiAgJHNjb3BlLnJ1bkpvYiA9ICgpIC0+XHJcbiAgICBpZiAkc2NvcGUuc3RhdGVbJ3N1Ym1pdC1idXR0b24nXSA9PSBcIlN1Ym1pdFwiXHJcbiAgICAgIGFjdGlvbiA9IG5ldyBEYXRlKCkuZ2V0VGltZSgpXHJcbiAgICAgICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSA9IGFjdGlvblxyXG4gICAgICAkc2NvcGUuc3RhdGVbJ3N1Ym1pdC1idXR0b24nXSA9IFwiU3VibWl0dGluZ1wiXHJcbiAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiU2hvdyBQbGFuXCJcclxuICAgICAgJHNjb3BlLmVycm9yID0gbnVsbFxyXG4gICAgICBKb2JTdWJtaXRTZXJ2aWNlLnJ1bkpvYihcclxuICAgICAgICAkc2NvcGUuc3RhdGUuc2VsZWN0ZWQsIHtcclxuICAgICAgICAgICdlbnRyeS1jbGFzcyc6ICRzY29wZS5zdGF0ZVsnZW50cnktY2xhc3MnXSxcclxuICAgICAgICAgIHBhcmFsbGVsaXNtOiAkc2NvcGUuc3RhdGUucGFyYWxsZWxpc20sXHJcbiAgICAgICAgICAncHJvZ3JhbS1hcmdzJzogJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXSxcclxuICAgICAgICAgIHNhdmVwb2ludFBhdGg6ICRzY29wZS5zdGF0ZVsnc2F2ZXBvaW50UGF0aCddLFxyXG4gICAgICAgICAgYWxsb3dOb25SZXN0b3JlZFN0YXRlOiAkc2NvcGUuc3RhdGVbJ2FsbG93Tm9uUmVzdG9yZWRTdGF0ZSddXHJcbiAgICAgICAgfVxyXG4gICAgICApLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICAgaWYgYWN0aW9uID09ICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXVxyXG4gICAgICAgICAgJHNjb3BlLnN0YXRlWydzdWJtaXQtYnV0dG9uJ10gPSBcIlN1Ym1pdFwiXHJcbiAgICAgICAgICAkc2NvcGUuZXJyb3IgPSBkYXRhLmVycm9yXHJcbiAgICAgICAgICBpZiBkYXRhLmpvYmlkP1xyXG4gICAgICAgICAgICAkc3RhdGUuZ28oXCJzaW5nbGUtam9iLnBsYW4uc3VidGFza3NcIiwge2pvYmlkOiBkYXRhLmpvYmlkfSlcclxuXHJcbiAgIyBqb2IgcGxhbiBkaXNwbGF5IHJlbGF0ZWQgc3R1ZmZcclxuICAkc2NvcGUubm9kZWlkID0gbnVsbFxyXG4gICRzY29wZS5jaGFuZ2VOb2RlID0gKG5vZGVpZCkgLT5cclxuICAgIGlmIG5vZGVpZCAhPSAkc2NvcGUubm9kZWlkXHJcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWRcclxuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGxcclxuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxyXG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxyXG5cclxuICAgICAgJHNjb3BlLiRicm9hZGNhc3QgJ3JlbG9hZCdcclxuXHJcbiAgICBlbHNlXHJcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsXHJcbiAgICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxyXG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbFxyXG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXHJcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXHJcblxyXG4gICRzY29wZS5jbGVhckZpbGVzID0gKCkgLT5cclxuICAgICRzY29wZS51cGxvYWRlciA9IHt9XHJcblxyXG4gICRzY29wZS51cGxvYWRGaWxlcyA9IChmaWxlcykgLT5cclxuICAgICMgbWFrZSBzdXJlIGV2ZXJ5dGhpbmcgaXMgY2xlYXIgYWdhaW4uXHJcbiAgICAkc2NvcGUudXBsb2FkZXIgPSB7fVxyXG4gICAgaWYgZmlsZXMubGVuZ3RoID09IDFcclxuICAgICAgJHNjb3BlLnVwbG9hZGVyWydmaWxlJ10gPSBmaWxlc1swXVxyXG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ3VwbG9hZCddID0gdHJ1ZVxyXG4gICAgZWxzZVxyXG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ2Vycm9yJ10gPSBcIkRpZCB5YSBmb3JnZXQgdG8gc2VsZWN0IGEgZmlsZT9cIlxyXG5cclxuICAkc2NvcGUuc3RhcnRVcGxvYWQgPSAoKSAtPlxyXG4gICAgaWYgJHNjb3BlLnVwbG9hZGVyWydmaWxlJ10/XHJcbiAgICAgIGZvcm1kYXRhID0gbmV3IEZvcm1EYXRhKClcclxuICAgICAgZm9ybWRhdGEuYXBwZW5kKFwiamFyZmlsZVwiLCAkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSlcclxuICAgICAgJHNjb3BlLnVwbG9hZGVyWyd1cGxvYWQnXSA9IGZhbHNlXHJcbiAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gXCJJbml0aWFsaXppbmcgdXBsb2FkLi4uXCJcclxuICAgICAgeGhyID0gbmV3IFhNTEh0dHBSZXF1ZXN0KClcclxuICAgICAgeGhyLnVwbG9hZC5vbnByb2dyZXNzID0gKGV2ZW50KSAtPlxyXG4gICAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gbnVsbFxyXG4gICAgICAgICRzY29wZS51cGxvYWRlclsncHJvZ3Jlc3MnXSA9IHBhcnNlSW50KDEwMCAqIGV2ZW50LmxvYWRlZCAvIGV2ZW50LnRvdGFsKVxyXG4gICAgICB4aHIudXBsb2FkLm9uZXJyb3IgPSAoZXZlbnQpIC0+XHJcbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gbnVsbFxyXG4gICAgICAgICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IFwiQW4gZXJyb3Igb2NjdXJyZWQgd2hpbGUgdXBsb2FkaW5nIHlvdXIgZmlsZVwiXHJcbiAgICAgIHhoci51cGxvYWQub25sb2FkID0gKGV2ZW50KSAtPlxyXG4gICAgICAgICRzY29wZS51cGxvYWRlclsncHJvZ3Jlc3MnXSA9IG51bGxcclxuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiU2F2aW5nLi4uXCJcclxuICAgICAgeGhyLm9ucmVhZHlzdGF0ZWNoYW5nZSA9ICgpIC0+XHJcbiAgICAgICAgaWYgeGhyLnJlYWR5U3RhdGUgPT0gNFxyXG4gICAgICAgICAgcmVzcG9uc2UgPSBKU09OLnBhcnNlKHhoci5yZXNwb25zZVRleHQpXHJcbiAgICAgICAgICBpZiByZXNwb25zZS5lcnJvcj9cclxuICAgICAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydlcnJvciddID0gcmVzcG9uc2UuZXJyb3JcclxuICAgICAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBudWxsXHJcbiAgICAgICAgICBlbHNlXHJcbiAgICAgICAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gXCJVcGxvYWRlZCFcIlxyXG4gICAgICB4aHIub3BlbihcIlBPU1RcIiwgXCIvamFycy91cGxvYWRcIilcclxuICAgICAgeGhyLnNlbmQoZm9ybWRhdGEpXHJcbiAgICBlbHNlXHJcbiAgICAgIGNvbnNvbGUubG9nKFwiVW5leHBlY3RlZCBFcnJvci4gVGhpcyBzaG91bGQgbm90IGhhcHBlblwiKVxyXG5cclxuLmZpbHRlciAnZ2V0SmFyU2VsZWN0Q2xhc3MnLCAtPlxyXG4gIChzZWxlY3RlZCwgYWN0dWFsKSAtPlxyXG4gICAgaWYgc2VsZWN0ZWQgPT0gYWN0dWFsXHJcbiAgICAgIFwiZmEtY2hlY2stc3F1YXJlXCJcclxuICAgIGVsc2VcclxuICAgICAgXCJmYS1zcXVhcmUtb1wiXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ0pvYlN1Ym1pdENvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYlN1Ym1pdFNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcsICRzdGF0ZSwgJGxvY2F0aW9uKSB7XG4gIHZhciByZWZyZXNoO1xuICAkc2NvcGUueWFybiA9ICRsb2NhdGlvbi5hYnNVcmwoKS5pbmRleE9mKFwiL3Byb3h5L2FwcGxpY2F0aW9uX1wiKSAhPT0gLTE7XG4gICRzY29wZS5sb2FkTGlzdCA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JTdWJtaXRTZXJ2aWNlLmxvYWRKYXJMaXN0KCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAkc2NvcGUuYWRkcmVzcyA9IGRhdGEuYWRkcmVzcztcbiAgICAgICRzY29wZS5ub2FjY2VzcyA9IGRhdGEuZXJyb3I7XG4gICAgICByZXR1cm4gJHNjb3BlLmphcnMgPSBkYXRhLmZpbGVzO1xuICAgIH0pO1xuICB9O1xuICAkc2NvcGUuZGVmYXVsdFN0YXRlID0gZnVuY3Rpb24oKSB7XG4gICAgJHNjb3BlLnBsYW4gPSBudWxsO1xuICAgICRzY29wZS5lcnJvciA9IG51bGw7XG4gICAgcmV0dXJuICRzY29wZS5zdGF0ZSA9IHtcbiAgICAgIHNlbGVjdGVkOiBudWxsLFxuICAgICAgcGFyYWxsZWxpc206IFwiXCIsXG4gICAgICBzYXZlcG9pbnRQYXRoOiBcIlwiLFxuICAgICAgYWxsb3dOb25SZXN0b3JlZFN0YXRlOiBmYWxzZSxcbiAgICAgICdlbnRyeS1jbGFzcyc6IFwiXCIsXG4gICAgICAncHJvZ3JhbS1hcmdzJzogXCJcIixcbiAgICAgICdwbGFuLWJ1dHRvbic6IFwiU2hvdyBQbGFuXCIsXG4gICAgICAnc3VibWl0LWJ1dHRvbic6IFwiU3VibWl0XCIsXG4gICAgICAnYWN0aW9uLXRpbWUnOiAwXG4gICAgfTtcbiAgfTtcbiAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpO1xuICAkc2NvcGUudXBsb2FkZXIgPSB7fTtcbiAgJHNjb3BlLmxvYWRMaXN0KCk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5sb2FkTGlzdCgpO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbiAgJHNjb3BlLnNlbGVjdEphciA9IGZ1bmN0aW9uKGlkKSB7XG4gICAgaWYgKCRzY29wZS5zdGF0ZS5zZWxlY3RlZCA9PT0gaWQpIHtcbiAgICAgIHJldHVybiAkc2NvcGUuZGVmYXVsdFN0YXRlKCk7XG4gICAgfSBlbHNlIHtcbiAgICAgICRzY29wZS5kZWZhdWx0U3RhdGUoKTtcbiAgICAgIHJldHVybiAkc2NvcGUuc3RhdGUuc2VsZWN0ZWQgPSBpZDtcbiAgICB9XG4gIH07XG4gICRzY29wZS5kZWxldGVKYXIgPSBmdW5jdGlvbihldmVudCwgaWQpIHtcbiAgICBpZiAoJHNjb3BlLnN0YXRlLnNlbGVjdGVkID09PSBpZCkge1xuICAgICAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpO1xuICAgIH1cbiAgICBhbmd1bGFyLmVsZW1lbnQoZXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJmYS1yZW1vdmVcIikuYWRkQ2xhc3MoXCJmYS1zcGluIGZhLXNwaW5uZXJcIik7XG4gICAgcmV0dXJuIEpvYlN1Ym1pdFNlcnZpY2UuZGVsZXRlSmFyKGlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChldmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImZhLXNwaW4gZmEtc3Bpbm5lclwiKS5hZGRDbGFzcyhcImZhLXJlbW92ZVwiKTtcbiAgICAgIGlmIChkYXRhLmVycm9yICE9IG51bGwpIHtcbiAgICAgICAgcmV0dXJuIGFsZXJ0KGRhdGEuZXJyb3IpO1xuICAgICAgfVxuICAgIH0pO1xuICB9O1xuICAkc2NvcGUubG9hZEVudHJ5Q2xhc3MgPSBmdW5jdGlvbihuYW1lKSB7XG4gICAgcmV0dXJuICRzY29wZS5zdGF0ZVsnZW50cnktY2xhc3MnXSA9IG5hbWU7XG4gIH07XG4gICRzY29wZS5nZXRQbGFuID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGFjdGlvbjtcbiAgICBpZiAoJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID09PSBcIlNob3cgUGxhblwiKSB7XG4gICAgICBhY3Rpb24gPSBuZXcgRGF0ZSgpLmdldFRpbWUoKTtcbiAgICAgICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSA9IGFjdGlvbjtcbiAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIjtcbiAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiR2V0dGluZyBQbGFuXCI7XG4gICAgICAkc2NvcGUuZXJyb3IgPSBudWxsO1xuICAgICAgJHNjb3BlLnBsYW4gPSBudWxsO1xuICAgICAgcmV0dXJuIEpvYlN1Ym1pdFNlcnZpY2UuZ2V0UGxhbigkc2NvcGUuc3RhdGUuc2VsZWN0ZWQsIHtcbiAgICAgICAgJ2VudHJ5LWNsYXNzJzogJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddLFxuICAgICAgICBwYXJhbGxlbGlzbTogJHNjb3BlLnN0YXRlLnBhcmFsbGVsaXNtLFxuICAgICAgICAncHJvZ3JhbS1hcmdzJzogJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXVxuICAgICAgfSkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIGlmIChhY3Rpb24gPT09ICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSkge1xuICAgICAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiU2hvdyBQbGFuXCI7XG4gICAgICAgICAgJHNjb3BlLmVycm9yID0gZGF0YS5lcnJvcjtcbiAgICAgICAgICByZXR1cm4gJHNjb3BlLnBsYW4gPSBkYXRhLnBsYW47XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLnJ1bkpvYiA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBhY3Rpb247XG4gICAgaWYgKCRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID09PSBcIlN1Ym1pdFwiKSB7XG4gICAgICBhY3Rpb24gPSBuZXcgRGF0ZSgpLmdldFRpbWUoKTtcbiAgICAgICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSA9IGFjdGlvbjtcbiAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXR0aW5nXCI7XG4gICAgICAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPSBcIlNob3cgUGxhblwiO1xuICAgICAgJHNjb3BlLmVycm9yID0gbnVsbDtcbiAgICAgIHJldHVybiBKb2JTdWJtaXRTZXJ2aWNlLnJ1bkpvYigkc2NvcGUuc3RhdGUuc2VsZWN0ZWQsIHtcbiAgICAgICAgJ2VudHJ5LWNsYXNzJzogJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddLFxuICAgICAgICBwYXJhbGxlbGlzbTogJHNjb3BlLnN0YXRlLnBhcmFsbGVsaXNtLFxuICAgICAgICAncHJvZ3JhbS1hcmdzJzogJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXSxcbiAgICAgICAgc2F2ZXBvaW50UGF0aDogJHNjb3BlLnN0YXRlWydzYXZlcG9pbnRQYXRoJ10sXG4gICAgICAgIGFsbG93Tm9uUmVzdG9yZWRTdGF0ZTogJHNjb3BlLnN0YXRlWydhbGxvd05vblJlc3RvcmVkU3RhdGUnXVxuICAgICAgfSkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIGlmIChhY3Rpb24gPT09ICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSkge1xuICAgICAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIjtcbiAgICAgICAgICAkc2NvcGUuZXJyb3IgPSBkYXRhLmVycm9yO1xuICAgICAgICAgIGlmIChkYXRhLmpvYmlkICE9IG51bGwpIHtcbiAgICAgICAgICAgIHJldHVybiAkc3RhdGUuZ28oXCJzaW5nbGUtam9iLnBsYW4uc3VidGFza3NcIiwge1xuICAgICAgICAgICAgICBqb2JpZDogZGF0YS5qb2JpZFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG4gICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAkc2NvcGUuY2hhbmdlTm9kZSA9IGZ1bmN0aW9uKG5vZGVpZCkge1xuICAgIGlmIChub2RlaWQgIT09ICRzY29wZS5ub2RlaWQpIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWQ7XG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbDtcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGw7XG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUuJGJyb2FkY2FzdCgncmVsb2FkJyk7XG4gICAgfSBlbHNlIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAgICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGw7XG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICAgcmV0dXJuICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLmNsZWFyRmlsZXMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyID0ge307XG4gIH07XG4gICRzY29wZS51cGxvYWRGaWxlcyA9IGZ1bmN0aW9uKGZpbGVzKSB7XG4gICAgJHNjb3BlLnVwbG9hZGVyID0ge307XG4gICAgaWYgKGZpbGVzLmxlbmd0aCA9PT0gMSkge1xuICAgICAgJHNjb3BlLnVwbG9hZGVyWydmaWxlJ10gPSBmaWxlc1swXTtcbiAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3VwbG9hZCddID0gdHJ1ZTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IFwiRGlkIHlhIGZvcmdldCB0byBzZWxlY3QgYSBmaWxlP1wiO1xuICAgIH1cbiAgfTtcbiAgcmV0dXJuICRzY29wZS5zdGFydFVwbG9hZCA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBmb3JtZGF0YSwgeGhyO1xuICAgIGlmICgkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSAhPSBudWxsKSB7XG4gICAgICBmb3JtZGF0YSA9IG5ldyBGb3JtRGF0YSgpO1xuICAgICAgZm9ybWRhdGEuYXBwZW5kKFwiamFyZmlsZVwiLCAkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSk7XG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ3VwbG9hZCddID0gZmFsc2U7XG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiSW5pdGlhbGl6aW5nIHVwbG9hZC4uLlwiO1xuICAgICAgeGhyID0gbmV3IFhNTEh0dHBSZXF1ZXN0KCk7XG4gICAgICB4aHIudXBsb2FkLm9ucHJvZ3Jlc3MgPSBmdW5jdGlvbihldmVudCkge1xuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IG51bGw7XG4gICAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3Byb2dyZXNzJ10gPSBwYXJzZUludCgxMDAgKiBldmVudC5sb2FkZWQgLyBldmVudC50b3RhbCk7XG4gICAgICB9O1xuICAgICAgeGhyLnVwbG9hZC5vbmVycm9yID0gZnVuY3Rpb24oZXZlbnQpIHtcbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gbnVsbDtcbiAgICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IFwiQW4gZXJyb3Igb2NjdXJyZWQgd2hpbGUgdXBsb2FkaW5nIHlvdXIgZmlsZVwiO1xuICAgICAgfTtcbiAgICAgIHhoci51cGxvYWQub25sb2FkID0gZnVuY3Rpb24oZXZlbnQpIHtcbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gbnVsbDtcbiAgICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gXCJTYXZpbmcuLi5cIjtcbiAgICAgIH07XG4gICAgICB4aHIub25yZWFkeXN0YXRlY2hhbmdlID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciByZXNwb25zZTtcbiAgICAgICAgaWYgKHhoci5yZWFkeVN0YXRlID09PSA0KSB7XG4gICAgICAgICAgcmVzcG9uc2UgPSBKU09OLnBhcnNlKHhoci5yZXNwb25zZVRleHQpO1xuICAgICAgICAgIGlmIChyZXNwb25zZS5lcnJvciAhPSBudWxsKSB7XG4gICAgICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ2Vycm9yJ10gPSByZXNwb25zZS5lcnJvcjtcbiAgICAgICAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IG51bGw7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiVXBsb2FkZWQhXCI7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgeGhyLm9wZW4oXCJQT1NUXCIsIFwiL2phcnMvdXBsb2FkXCIpO1xuICAgICAgcmV0dXJuIHhoci5zZW5kKGZvcm1kYXRhKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIGNvbnNvbGUubG9nKFwiVW5leHBlY3RlZCBFcnJvci4gVGhpcyBzaG91bGQgbm90IGhhcHBlblwiKTtcbiAgICB9XG4gIH07XG59KS5maWx0ZXIoJ2dldEphclNlbGVjdENsYXNzJywgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbihzZWxlY3RlZCwgYWN0dWFsKSB7XG4gICAgaWYgKHNlbGVjdGVkID09PSBhY3R1YWwpIHtcbiAgICAgIHJldHVybiBcImZhLWNoZWNrLXNxdWFyZVwiO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gXCJmYS1zcXVhcmUtb1wiO1xuICAgIH1cbiAgfTtcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4uc2VydmljZSAnSm9iU3VibWl0U2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxyXG5cclxuICBAbG9hZEphckxpc3QgPSAoKSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiamFycy9cIilcclxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGRlbGV0ZUphciA9IChpZCkgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmRlbGV0ZShmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImphcnMvXCIgKyBlbmNvZGVVUklDb21wb25lbnQoaWQpKVxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGdldFBsYW4gPSAoaWQsIGFyZ3MpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqYXJzL1wiICsgZW5jb2RlVVJJQ29tcG9uZW50KGlkKSArIFwiL3BsYW5cIiwge3BhcmFtczogYXJnc30pXHJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XHJcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBydW5Kb2IgPSAoaWQsIGFyZ3MpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5wb3N0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiamFycy9cIiArIGVuY29kZVVSSUNvbXBvbmVudChpZCkgKyBcIi9ydW5cIiwge30sIHtwYXJhbXM6IGFyZ3N9KVxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ0pvYlN1Ym1pdFNlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHRoaXMubG9hZEphckxpc3QgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImphcnMvXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmRlbGV0ZUphciA9IGZ1bmN0aW9uKGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cFtcImRlbGV0ZVwiXShmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImphcnMvXCIgKyBlbmNvZGVVUklDb21wb25lbnQoaWQpKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRQbGFuID0gZnVuY3Rpb24oaWQsIGFyZ3MpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImphcnMvXCIgKyBlbmNvZGVVUklDb21wb25lbnQoaWQpICsgXCIvcGxhblwiLCB7XG4gICAgICBwYXJhbXM6IGFyZ3NcbiAgICB9KS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5ydW5Kb2IgPSBmdW5jdGlvbihpZCwgYXJncykge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAucG9zdChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImphcnMvXCIgKyBlbmNvZGVVUklDb21wb25lbnQoaWQpICsgXCIvcnVuXCIsIHt9LCB7XG4gICAgICBwYXJhbXM6IGFyZ3NcbiAgICB9KS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLmNvbnRyb2xsZXIgJ0FsbFRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBUYXNrTWFuYWdlcnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxyXG4gIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbiAoZGF0YSkgLT5cclxuICAgICRzY29wZS5tYW5hZ2VycyA9IGRhdGFcclxuXHJcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbCAtPlxyXG4gICAgVGFza01hbmFnZXJzU2VydmljZS5sb2FkTWFuYWdlcnMoKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAkc2NvcGUubWFuYWdlcnMgPSBkYXRhXHJcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cclxuXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxyXG5cclxuLmNvbnRyb2xsZXIgJ1NpbmdsZVRhc2tNYW5hZ2VyQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZVBhcmFtcywgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxyXG4gICRzY29wZS5tZXRyaWNzID0ge31cclxuICBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZE1ldHJpY3MoJHN0YXRlUGFyYW1zLnRhc2ttYW5hZ2VyaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS5tZXRyaWNzID0gZGF0YVswXVxyXG5cclxuICAgIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cclxuICAgICAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRNZXRyaWNzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAgICRzY29wZS5tZXRyaWNzID0gZGF0YVswXVxyXG4gICAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cclxuXHJcbiAgICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XHJcbiAgICAgICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaClcclxuXHJcbi5jb250cm9sbGVyICdTaW5nbGVUYXNrTWFuYWdlckxvZ3NDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlUGFyYW1zLCBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIC0+XHJcbiAgJHNjb3BlLmxvZyA9IHt9XHJcbiAgJHNjb3BlLnRhc2ttYW5hZ2VyaWQgPSAkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZFxyXG4gIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZS5sb2FkTG9ncygkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbiAoZGF0YSkgLT5cclxuICAgICRzY29wZS5sb2cgPSBkYXRhXHJcblxyXG4gICRzY29wZS5yZWxvYWREYXRhID0gKCkgLT5cclxuICAgIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZS5sb2FkTG9ncygkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgJHNjb3BlLmxvZyA9IGRhdGFcclxuXHJcbi5jb250cm9sbGVyICdTaW5nbGVUYXNrTWFuYWdlclN0ZG91dENvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGVQYXJhbXMsIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykgLT5cclxuICAkc2NvcGUuc3Rkb3V0ID0ge31cclxuICAkc2NvcGUudGFza21hbmFnZXJpZCA9ICRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkXHJcbiAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRTdGRvdXQoJHN0YXRlUGFyYW1zLnRhc2ttYW5hZ2VyaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAkc2NvcGUuc3Rkb3V0ID0gZGF0YVxyXG5cclxuICAkc2NvcGUucmVsb2FkRGF0YSA9ICgpIC0+XHJcbiAgICBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZFN0ZG91dCgkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgJHNjb3BlLnN0ZG91dCA9IGRhdGFcclxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignQWxsVGFza01hbmFnZXJzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgVGFza01hbmFnZXJzU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykge1xuICB2YXIgcmVmcmVzaDtcbiAgVGFza01hbmFnZXJzU2VydmljZS5sb2FkTWFuYWdlcnMoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLm1hbmFnZXJzID0gZGF0YTtcbiAgfSk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLm1hbmFnZXJzID0gZGF0YTtcbiAgICB9KTtcbiAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgcmV0dXJuICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaCk7XG4gIH0pO1xufSkuY29udHJvbGxlcignU2luZ2xlVGFza01hbmFnZXJDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGVQYXJhbXMsIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykge1xuICB2YXIgcmVmcmVzaDtcbiAgJHNjb3BlLm1ldHJpY3MgPSB7fTtcbiAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRNZXRyaWNzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLm1ldHJpY3MgPSBkYXRhWzBdO1xuICB9KTtcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRNZXRyaWNzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUubWV0cmljcyA9IGRhdGFbMF07XG4gICAgfSk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZVRhc2tNYW5hZ2VyTG9nc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZVBhcmFtcywgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSB7XG4gICRzY29wZS5sb2cgPSB7fTtcbiAgJHNjb3BlLnRhc2ttYW5hZ2VyaWQgPSAkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZDtcbiAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRMb2dzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLmxvZyA9IGRhdGE7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLnJlbG9hZERhdGEgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRMb2dzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUubG9nID0gZGF0YTtcbiAgICB9KTtcbiAgfTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZVRhc2tNYW5hZ2VyU3Rkb3V0Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlUGFyYW1zLCBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgJHNjb3BlLnN0ZG91dCA9IHt9O1xuICAkc2NvcGUudGFza21hbmFnZXJpZCA9ICRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkO1xuICBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZFN0ZG91dCgkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5zdGRvdXQgPSBkYXRhO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS5yZWxvYWREYXRhID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZS5sb2FkU3Rkb3V0KCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUuc3Rkb3V0ID0gZGF0YTtcbiAgICB9KTtcbiAgfTtcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4uc2VydmljZSAnVGFza01hbmFnZXJzU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxyXG4gIEBsb2FkTWFuYWdlcnMgPSAoKSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwidGFza21hbmFnZXJzXCIpXHJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XHJcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YVsndGFza21hbmFnZXJzJ10pXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAXHJcblxyXG4uc2VydmljZSAnU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XHJcbiAgQGxvYWRNZXRyaWNzID0gKHRhc2ttYW5hZ2VyaWQpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJ0YXNrbWFuYWdlcnMvXCIgKyB0YXNrbWFuYWdlcmlkICsgXCIvbWV0cmljc1wiKVxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGFbJ3Rhc2ttYW5hZ2VycyddKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGxvYWRMb2dzID0gKHRhc2ttYW5hZ2VyaWQpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJ0YXNrbWFuYWdlcnMvXCIgKyB0YXNrbWFuYWdlcmlkICsgXCIvbG9nXCIpXHJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XHJcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBsb2FkU3Rkb3V0ID0gKHRhc2ttYW5hZ2VyaWQpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJ0YXNrbWFuYWdlcnMvXCIgKyB0YXNrbWFuYWdlcmlkICsgXCIvc3Rkb3V0XCIpXHJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XHJcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBcclxuXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ1Rhc2tNYW5hZ2Vyc1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHRoaXMubG9hZE1hbmFnZXJzID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJ0YXNrbWFuYWdlcnNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YVsndGFza21hbmFnZXJzJ10pO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pLnNlcnZpY2UoJ1NpbmdsZVRhc2tNYW5hZ2VyU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkTWV0cmljcyA9IGZ1bmN0aW9uKHRhc2ttYW5hZ2VyaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcInRhc2ttYW5hZ2Vycy9cIiArIHRhc2ttYW5hZ2VyaWQgKyBcIi9tZXRyaWNzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGFbJ3Rhc2ttYW5hZ2VycyddKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5sb2FkTG9ncyA9IGZ1bmN0aW9uKHRhc2ttYW5hZ2VyaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcInRhc2ttYW5hZ2Vycy9cIiArIHRhc2ttYW5hZ2VyaWQgKyBcIi9sb2dcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMubG9hZFN0ZG91dCA9IGZ1bmN0aW9uKHRhc2ttYW5hZ2VyaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcInRhc2ttYW5hZ2Vycy9cIiArIHRhc2ttYW5hZ2VyaWQgKyBcIi9zdGRvdXRcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iXSwic291cmNlUm9vdCI6Ii9zb3VyY2UvIn0=
