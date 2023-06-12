import React, { useState } from 'react'

import { PageHeader } from 'antd'

import ProjectTable from './components/ProjectTable'
import SearchBar from './components/SearchBar'

const Projects = () => {
  const [project, setProject] = useState<string>('')

  const onSearch = ({ project }: { project: string }) => {
    setProject(project)
  }

  return (
    <div className="page">
      <PageHeader title="项目列表" ghost={false}>
        <SearchBar onSearch={onSearch} />
        <ProjectTable project={project} />
      </PageHeader>
    </div>
  )
}

export default Projects
