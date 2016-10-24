import {Component, Input, ElementRef, Inject, Output, EventEmitter, Directive, OnInit}      from '@angular/core';

import 'rxjs/Rx';

import {CandidatesService, NerAnnotation, TwitterAnnotation, Score, ScoreBundle} from "../services/api";

@Component({
    selector: '[smt-candidate]',
    templateUrl: '../../templates/candidate.html',
    providers: [ ]
})
export class CandidateRow implements OnInit {
    @Input() candidate : Object;
    @Input() scores : Score[];
    private chosenScore : Score;

    scoreClick(score : Score) {
        if (typeof score.debug == 'undefined') {
            return;
        }
        this.chosenScore = score;
        console.log(score);
    }

    resetScore() {
        console.log("Reset!");
        this.chosenScore = null;
    }

    ngOnInit() { }

    constructor(private candidatesService : CandidatesService) {
        this.chosenScore = null;
    };
}

@Component({
    selector: 'smt-alignment-result',
    templateUrl: '../../templates/alignment-result.html',
    providers: [ ]
})
export class AlignmentResult {
    @Input('annotation') private annotation : NerAnnotation;
    @Output() selected = new EventEmitter<NerAnnotation>();

    getClassName() {
        switch(this.annotation.nerClass) {
            case "PERSON":
                return "label label-success";
            case "ORGANIZATION":
                return "label label-primary";
            default:
                return "";
        }
    }

    selectAnnotation() {
        this.selected.emit(this.annotation);
    }
}

@Component({
    selector: 'smt-annotate',
    templateUrl: '../../templates/annotate.html',
    providers: [ CandidatesService ],
    viewProviders: [ AlignmentResult, CandidateRow ]
})
export class Annotate {
    private errorMessage : string;
    private annotations : NerAnnotation[];
    private selectedAnnotation : NerAnnotation;
    private twitterAnnotation : TwitterAnnotation;
    private annotationRepository : Candidate[];
    private scoreTypeDict = {
        "cos_sim+bow": "BOW",
        "cos_sim+bow_claudio": "BOW C",
        "cos_sim+lsa": "LSA",
        "alignments": "Align"
    };

    getTypeName(type : string) {
        if (typeof this.scoreTypeDict[type] == 'undefined') {
            return type;
        }
        return this.scoreTypeDict[type];
    }

    annotate(searchString : string) {
        let self = this;
        this.selectedAnnotation = null;
        this.twitterAnnotation = null;
        this.annotations = [];
        this.candidatesService.getNerAnnotation(searchString).subscribe(
            function (annotations) {
                self.annotations = annotations;
            },
            function (error) {
                self.annotations = null;
                self.errorMessage = <any>error;
            }
        );
    }

    loadingNer() {

    }

    loadingTwitter() {

    }

    selectAnnotation(text : string, annotation : NerAnnotation) {
        let self = this;
        this.selectedAnnotation = annotation;
        this.candidatesService.getTwitterAnnotation(text, this.selectedAnnotation.token, this.selectedAnnotation.nerClass).subscribe(
            function (annotation) {
                self.twitterAnnotation = annotation;
                let candidates = {};
                for (let scoreBundle of annotation.results) {
                    for (let score of scoreBundle.scores) {
                        if (typeof candidates[score.username] == 'undefined') {
                            candidates[score.username] = new Candidate(score.username);
                        }
                        let candidate = candidates[score.username];
                        candidate.scores.push({
                            type: scoreBundle.type,
                            score: score.score,
                            debug: score.debug
                        });
                    }
                }
                self.annotationRepository = [];
                for (let candidate of Object.values(candidates)) {
                    self.annotationRepository.push(candidate);
                }
            },
            function (error) {
                self.twitterAnnotation = null;
                self.errorMessage = <any>error;
            }
        )
    }

    sortScores(bundle : ScoreBundle) {
        if (typeof bundle["sorted"] == undefined) {
            bundle["sorted"] = false;
        }
        this.annotationRepository.sort(function (cand1 : Candidate, cand2 : Candidate) {
            let score1 = 0.0;
            let score2 = 0.0;
            for (let score of cand1.scores) {
                if (score["type"] == bundle.type) {
                    score1 = score["score"];
                }
            }
            for (let score of cand2.scores) {
                if (score["type"] == bundle.type) {
                    score2 = score["score"];
                }
            }

            let diff = score2 - score1;
            if (bundle["sorted"]) {
                diff = -diff;
            }
            return diff;
        });
        bundle["sorted"] = !bundle["sorted"];
    }

    constructor(private candidatesService : CandidatesService) {
        this.annotations = [];
        this.selectedAnnotation = null;
        this.twitterAnnotation = null;
    };
}

export class Candidate {
    public username : string;
    public scores : Object[];

    constructor(name : string) {
        this.username = name;
        this.scores = [];
    };
}