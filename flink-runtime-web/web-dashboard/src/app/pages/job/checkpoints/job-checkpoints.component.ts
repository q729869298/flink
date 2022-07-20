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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { forkJoin, of, Subject } from 'rxjs';
import { catchError, distinctUntilChanged, switchMap, takeUntil } from 'rxjs/operators';

import { CheckpointConfig, CheckpointHistory, Checkpoint, JobDetailCorrect } from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';

import { JobLocalService } from '../job-local.service';

@Component({
  selector: 'flink-job-checkpoints',
  templateUrl: './job-checkpoints.component.html',
  styleUrls: ['./job-checkpoints.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobCheckpointsComponent implements OnInit, OnDestroy {
  disabledInterval = 0x7fffffffffffffff;

  public readonly trackById = (_: number, node: CheckpointHistory): number => node.id;

  public checkPointStats?: Checkpoint;
  public checkPointConfig?: CheckpointConfig;
  public jobDetail: JobDetailCorrect;

  public moreDetailsPanel = { active: false, disabled: false };

  private refresh$ = new Subject<void>();
  private destroy$ = new Subject<void>();

  constructor(
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.refresh$
      .pipe(
        switchMap(() =>
          forkJoin([
            this.jobService.loadCheckpointStats(this.jobDetail.jid).pipe(
              catchError(() => {
                return of(undefined);
              })
            ),
            this.jobService.loadCheckpointConfig(this.jobDetail.jid).pipe(
              catchError(() => {
                return of(undefined);
              })
            )
          ])
        ),
        takeUntil(this.destroy$)
      )
      .subscribe(([stats, config]) => {
        this.checkPointStats = stats;
        this.checkPointConfig = config;
        this.cdr.markForCheck();
      });

    this.jobLocalService
      .jobDetailChanges()
      .pipe(
        distinctUntilChanged((pre, next) => pre.jid === next.jid),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.jobDetail = data;
        this.cdr.markForCheck();
        this.refresh$.next();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.refresh$.complete();
  }

  public refresh(): void {
    this.refresh$.next();
  }
}
