/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { of, Subject } from 'rxjs';
import { catchError, takeUntil } from 'rxjs/operators';

import {
  TASK_MANAGER_MODULE_CONFIG,
  TASK_MANAGER_MODULE_DEFAULT_CONFIG,
  TaskManagerModuleConfig
} from '@flink-runtime-web/pages/task-manager/task-manager.config';
import { ConfigService, TaskManagerService } from '@flink-runtime-web/services';
import { EditorOptions } from 'ng-zorro-antd/code-editor/typings';

@Component({
  selector: 'flink-task-manager-stdout',
  templateUrl: './task-manager-stdout.component.html',
  styleUrls: ['./task-manager-stdout.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerStdoutComponent implements OnInit, OnDestroy {
  public editorOptions: EditorOptions;
  public stdout = '';
  public loading = true;
  public taskManagerId: string;
  public downloadUrl = '';
  public downloadName = '';

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly taskManagerService: TaskManagerService,
    private readonly configService: ConfigService,
    private readonly activatedRoute: ActivatedRoute,
    private readonly cdr: ChangeDetectorRef,
    @Inject(TASK_MANAGER_MODULE_CONFIG) readonly moduleConfig: TaskManagerModuleConfig
  ) {
    this.editorOptions = moduleConfig.editorOptions || TASK_MANAGER_MODULE_DEFAULT_CONFIG.editorOptions;
  }

  public ngOnInit(): void {
    this.taskManagerId = this.activatedRoute.parent!.snapshot.params.taskManagerId;
    this.downloadUrl = `${this.configService.BASE_URL}/taskmanagers/${this.taskManagerId}/stdout`;
    this.downloadName = `taskmanager_${this.taskManagerId}_stdout`;
    this.reload();
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public reload(): void {
    this.loading = true;
    this.cdr.markForCheck();
    this.taskManagerService
      .loadStdout(this.taskManagerId)
      .pipe(
        catchError(() => of('')),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.stdout = data;
        this.loading = false;
        this.cdr.markForCheck();
      });
  }
}
