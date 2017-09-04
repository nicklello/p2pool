pipeline {
  agent any
  stages {
    stage('') {
      steps {
        parallel(
          "stage1": {
            echo 'stage1 step1'
            
          },
          "stage2": {
            echo 'stage2 step1'
            
          }
        )
      }
    }
    stage('after=1+2') {
      steps {
        echo 'done'
      }
    }
  }
}