import React, { useEffect, useRef, useState } from 'react'

import { PageHeader, Row, Col, Radio, Tabs } from 'antd'
import { useSearchParams } from 'react-router-dom'

import { fetchProjectLineages } from '@/api'
import FlowGraph from '@/components/FlowGraph'
import { observer, useStore } from '@/hooks'
import { FeatureLineage } from '@/models/model'
import { FeatureType } from '@/utils/utils'

import NodeDetails from '../Features/components/NodeDetails'

const items = [
  { label: '元数据', key: '1', children: <NodeDetails /> },
  { label: '指标', key: '2', children: <p>网页建设中</p> }, // 务必填写 key
  { label: '任务', key: '3', children: <p>网页建设中</p> }
]

const ProjectLineage = () => {
  const { globalStore } = useStore()
  const { project } = globalStore
  const [searchParams] = useSearchParams()
  const nodeId = searchParams.get('nodeId') as string

  const [lineageData, setLineageData] = useState<FeatureLineage>({
    guidEntityMap: {},
    relations: []
  })

  const [loading, setLoading] = useState<boolean>(false)

  const [featureType, setFeatureType] = useState<FeatureType>(FeatureType.AllNodes)

  const mountedRef = useRef<boolean>(true)

  // Fetch lineage data from server side, invoked immediately after component is mounted
  useEffect(() => {
    const fetchLineageData = async () => {
      setLoading(true)
      try {
        const data = await fetchProjectLineages(project)
        if (mountedRef.current) {
          setLineageData(data)
          setLoading(false)
        }
      } catch {
        //
      }
    }

    fetchLineageData()
  }, [project])

  const toggleFeatureType = (type: FeatureType) => {
    setFeatureType(type)
  }

  useEffect(() => {
    mountedRef.current = true
    return () => {
      mountedRef.current = false
    }
  }, [])

  return (
    <div className="page">
      <PageHeader ghost={false}>
        <Radio.Group value={featureType} onChange={(e) => toggleFeatureType(e.target.value)}>
          <Radio.Button value={FeatureType.AllNodes}>所有节点</Radio.Button>
          <Radio.Button value={FeatureType.Source}> 数据源 </Radio.Button>
          <Radio.Button value={FeatureType.AnchorFeature}>锚定特征</Radio.Button>
          <Radio.Button value={FeatureType.DerivedFeature}>衍生特征</Radio.Button>
        </Radio.Group>
        <Row>
          <Col flex="2">
            <FlowGraph
              minHeight="calc(100vh - 160px)"
              loading={loading}
              data={lineageData}
              nodeId={nodeId}
              project={project}
              featureType={featureType}
            />
          </Col>
          <Col flex="1">
            <Tabs defaultActiveKey="1" items={items} />
          </Col>
        </Row>
      </PageHeader>
    </div>
  )
}

export default observer(ProjectLineage)
